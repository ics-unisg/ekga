import json
import time
import uuid
import queue
import pandas as pd
import matplotlib.pyplot as plt
from neo4j import GraphDatabase

from utils.xes_log_reader import LogProcessor

from EKG_base import BaseEKG


#######################################################################################
#
# User defined
#
#######################################################################################
# Neo4j connection settings
uri = "neo4j://127.0.0.1:7687"
user = "neo4j"
password = "12345678"

# Streaming Source
USE_MQTT = False # False / "SenderReceiver" / "Receiver"
# MQTT settings (can leave empty with USE_MQTT=False)
mqtt_url = "ftsim.weber.ics.unisg.ch"
mqtt_topic = "smart-healthcare"
mqtt_port = 1883
mqtt_user = "ftsim"
mqtt_password = "unisg"

# User input
xes_file_path = "logs/extended_event_log_mock.xes" # *path* (for False / "SenderReceiver") / None (for USE_MQTT="Receiver")


#######################################################################################
#
# Run code
#
#######################################################################################
if __name__ == '__main__':

    processor = LogProcessor(log_path=xes_file_path, USE_MQTT=USE_MQTT,
                                              url=mqtt_url,
                                              topic=mqtt_topic,
                                              port=mqtt_port,
                                              user=mqtt_user,
                                              pw=mqtt_password)
    sender_alive = True

    processing_durations = []
    EKG = BaseEKG(uri, user, password)

    while True:
        # In a live MQTT setting we handle the termination via keyboard interrupt.
        # Since we are streaming to MQTT from the same file in our setup, we are
        # able to track the progress.
        if USE_MQTT == "SenderReceiver":
            if sender_alive and not processor.sender_thread.is_alive():
                print("Sender finished")
                sender_alive = False
            if not sender_alive and processor.message_queue.empty():
                print("Sender finished and Queue emptied")
                # Determine uuid
                uuid_nr = uuid.uuid4().hex
                # Store the runtime
                df = pd.DataFrame({"Duration": processing_durations})
                df.to_csv(f"results/durations_{xes_file_path.split('/')[-1]}_{uuid_nr}.csv", index=False)
                # Store the plots
                plt.figure(figsize=(10, 6))
                plt.plot(list(range(len(processing_durations))), processing_durations, marker='o')
                plt.xlabel('Event Index')
                plt.ylabel('Processing Time (seconds)')
                plt.title(f"Processing Time for Each Event in {xes_file_path.split('/')[-1]}")
                plt.grid(True)
                plt.savefig(f"results/durationsPlot_{xes_file_path.split('/')[-1]}_{uuid_nr}.png")
        elif USE_MQTT == "Receiver":
            # In our experiments we are storing the durations after the file is finished. We
            # could do the same for the Receiver only, but it does not change anything compared
            # to the SenderReceiver, which is why the storing functionality is not implemented
            # for the Receiver only.
            pass
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
                df.to_csv(f"results/durations_{xes_file_path.split('/')[-1]}_{uuid_nr}.csv", index=False)
                # Store the plots
                plt.figure(figsize=(10, 6))
                plt.plot(list(range(len(processing_durations))), processing_durations, marker='o')
                plt.xlabel('Event Index')
                plt.ylabel('Processing Time (seconds)')
                plt.title(f"Processing Time for Each Event in {xes_file_path.split('/')[-1]}")
                plt.grid(True)
                plt.savefig(f"results/durationsPlot_{xes_file_path.split('/')[-1]}_{uuid_nr}.png")
                # End
                break
        try:
            message = processor.message_queue.get(timeout=1)

            start_time = time.time()
            EKG.process_event(message)
            end_time = time.time()
            duration = end_time - start_time
            processing_durations.append(duration)
            print(duration)

            processor.message_queue.task_done()
        except queue.Empty:
            continue

    EKG.close()
