import json
import time
import uuid
import queue
import pandas as pd
import matplotlib.pyplot as plt

from xes_log_reader import LogProcessor

from neo4j import GraphDatabase
from EKG_base import BaseEKG


#######################################################################################
#
# User defined
#
#######################################################################################
# Neo4j connection settings
uri = "neo4j://127.0.0.1:7690"
user = "neo4j"
password = "12345678"

# User input
xes_file_path = "extended_event_log_6432.xes"
USE_MQTT = False


if __name__ == '__main__':

    processor = LogProcessor(log_path=xes_file_path, USE_MQTT=USE_MQTT)
    sender_alive = True

    # ------------> 1: G ← new EKGA; <------------
    processing_durations = []
    EKG = BaseEKG(uri, user, password)

    while True:
        # In a live MQTT setting we handle the termination via keyboard interrupt.
        # Since we are streaming to MQTT from the same file in our setup, we are
        # able to track the progress for convenience.
        if USE_MQTT:
            if sender_alive and not processor.sender_thread.is_alive():
                print("Sender finished")
                sender_alive = False
            if not sender_alive and processor.message_queue.empty():
                print("Sender finished and Queue emptied")
                # Determine uuid
                uuid_nr = uuid.uuid4().hex
                # Store the runtime
                df = pd.DataFrame({"Duration": processing_durations})
                df.to_csv(f"results/durations_{xes_file_path}_{uuid_nr}.csv", index=False)
                # Store the plots
                plt.figure(figsize=(10, 6))
                plt.plot(list(range(len(processing_durations))), processing_durations, marker='o')
                plt.xlabel('Event Index')
                plt.ylabel('Processing Time (seconds)')
                plt.title('Processing Time for Each Event')
                plt.grid(True)
                plt.savefig(f"results/durationsPlot_{xes_file_path}_{uuid_nr}.png")
        # Streaming from a file, we wait that the full file has been sent and queue emptied.
        else:
            if sender_alive and not processor.sender_thread.is_alive():
                print("Sender finished")
                sender_alive = False
            if not sender_alive and processor.message_queue.empty():
                print("Sender finished and Queue emptied")
                # Determine uuid
                uuid_nr = uuid.uuid4().hex
                # Store the runtime
                df = pd.DataFrame({"Duration": processing_durations})
                df.to_csv(f"results/durations_{xes_file_path}_{uuid_nr}.csv", index=False)
                # Store the plots
                plt.figure(figsize=(10, 6))
                plt.plot(list(range(len(processing_durations))), processing_durations, marker='o')
                plt.xlabel('Event Index')
                plt.ylabel('Processing Time (seconds)')
                plt.title(f'Processing Time for Each Event in {xes_file_path}')
                plt.grid(True)
                plt.savefig(f"results/durationsPlot_{xes_file_path}_{uuid_nr}.png")
                # End
                break
        try:
            message = processor.message_queue.get(timeout=1)
            # ------------> 2: for all event e ∈ s do <------------
            start_time = time.time()
            EKG.process_event(message)
            end_time = time.time()
            duration = end_time - start_time
            processing_durations.append(duration)
            print(duration)
            #input("Hit Enter to proceed") # allow step-by-step construction
            # ------------> 14: end for <------------
            processor.message_queue.task_done()
        except queue.Empty:
            continue

    EKG.close()
