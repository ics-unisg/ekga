from multiprocessing import Process, Queue
import time

class SharedMemoryWriter:
    def __init__(self, queue):
        self.queue = queue

    def write_message(self, message):
        self.queue.put(message)

class SharedMemoryReader:
    def __init__(self, queue):
        self.queue = queue

    def read_message(self):
        while True:
            message = self.queue.get()
            if message:
                print(f'Received message: {message}')
                self.process_message(message)

    def process_message(self, message):
        time.sleep(5)  # Simulate processing time


def external_writer(queue):
    writer = SharedMemoryWriter(queue)
    time.sleep(10)
    try:
        for i in range(10):
            writer.write_message(f'Hello {i} from External Writer')
            time.sleep(2)  # Simulate irregular message sending
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    message_queue = Queue()

    reader = SharedMemoryReader(message_queue)

    reader_process = Process(target=reader.read_message)
    reader_process.start()

    writer_process = Process(target=external_writer, args=(message_queue,))
    writer_process.start()

    writer_process.join()
    reader_process.join()




if __name__ == "__main__":
    message_queue = Queue()

    writer = SharedMemoryWriter(message_queue)
    reader = SharedMemoryReader(message_queue)

    writer_process = Process(target=writer.start_writing)
    reader_process = Process(target=reader.read_message)

    writer_process.start()
    reader_process.start()

    writer_process.join()
    reader_process.join()
