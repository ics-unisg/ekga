################################################################################################
#                                                                                                                                                                                            #
# Author: Dominik Manuel Buchegger                                                                                                                                #
#                                                                                                                                                                                            #
################################################################################################
import xml.etree.ElementTree as ET
import time
import json
import threading
import queue

from utils.mqtt_client import XESMQTTProducer, XESMQTTConsumer


class LogReader:

    def __init__(self, log_path):
        with open(log_path, "r") as f:
            log = f.read()

        # Remove specific namespaces if they are part of the log, e.g.,
        # here ns0 would be removed from the log.
        log = log.replace("ns0:", "")
        log = log.replace(":ns0", "")

        # Parse the XML data
        namespace = {"xes": "http://www.xes-standard.org/"}
        root = ET.fromstring(log)

        # List to store parsed events
        self.events = []

        # Helper function to parse nested containers
        def parse_container(container_element):
            container_data = {}
            for entry in container_element.findall("xes:container", namespace):
                entry_data = {}
                for child in entry:
                    key = child.attrib.get("key")
                    value = child.attrib.get("value")
                    if key and value:
                        entry_data[key] = value
                container_data.setdefault("uncertainty:entries", []).append(entry_data)
            return container_data

        # Extract each event as a dictionary
        for event in root.findall(".//xes:event", namespace):
            event_data = {}
            # Loop through child elements within each event
            for child in event:
                key = child.attrib.get("key")
                value = child.attrib.get("value")
                # If it's a container, parse its contents
                if child.tag.endswith("container") and key == "uncertainty:discrete_weak":
                    event_data["uncertainty:discrete_weak"] = parse_container(child)
                elif key and value:
                    # Regular key-value elements
                    event_data[key] = value
            self.events.append(event_data)

        # Store all values
        self.to_return = []
        # Iterate through all events
        for unparsed_event in self.events:
            event_keys = list(unparsed_event.keys())

            # name
            name = unparsed_event["concept:name"]
            event_keys.remove("concept:name")
            # time
            timestamp = unparsed_event["time:timestamp"]
            event_keys.remove("time:timestamp")
            # probability
            probability = 1.0
            # attributes list
            attributes = []

            # Add uncertainty to the attributes list if existing
            if "uncertainty:discrete_weak" in event_keys:
                # Parse the uncertainty block
                event_keys.remove("uncertainty:discrete_weak")
                uncertain_box = unparsed_event["uncertainty:discrete_weak"]["uncertainty:entries"]
                indeterminancy = False
                # Iterate through all uncertainty entries in the block
                for uncertain_dict in uncertain_box:
                    uncertain_dict_keys = list(uncertain_dict.keys())
                    # Event uncertainty
                    if "uncertainty:indeterminacy" in uncertain_dict:
                        # check log structure
                        if indeterminancy:
                            print(unparsed_event)
                            raise ValueError("Cannot have multiple indeterminancies in one uncertainty box!")
                        # add event uncertainty to attributes list
                        indeterminancy = True
                        probability = uncertain_dict["uncertainty:probability"]
                        uncertain_dict_keys.remove("uncertainty:probability")
                    # Attribute uncertainty
                    else:
                        # extract attribute uncertainty
                        attribute_probability = float(uncertain_dict["uncertainty:probability"])
                        uncertain_dict_keys.remove("uncertainty:probability")
                        first_attributes = uncertain_dict_keys[0].split(":")
                        # check log structure
                        if len(first_attributes) != 2:
                            print(unparsed_event)
                            raise ValueError("Attribute name cannot contain : !")
                        # add attribute uncertainty to attributes list
                        attributes.append((first_attributes[0], first_attributes[1], uncertain_dict[uncertain_dict_keys[0]], attribute_probability))

            # Add all attributes to the attributes list
            for key in event_keys:
                # extract attribute
                first_attributes = key.split(":")
                # check log structure
                if len(first_attributes) != 2:
                    print(unparsed_event)
                    raise ValueError("Attribute name cannot contain : !")
                # add attribute to attributes list
                attributes.append((first_attributes[0], first_attributes[1], unparsed_event[key], 1.0))

            # Create an EventXES object
            event = EventXES(name, timestamp, probability, attributes)
            self.to_return.append(event)

        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < len(self.to_return):
            event = self.to_return[self.index]
            self.index += 1
            return event
        else:
            raise StopIteration

class EventXES:

    def __init__(self, name, timestamp, probability, attributes):
        self._name = name
        self._timestamp = timestamp
        self._probability = probability
        self._attributes = attributes

    @property
    def name(self) -> str:
        """Get the event name."""
        return self._name

    @property
    def timestamp(self) -> str:
        """Get the event timestamp."""
        return self._timestamp

    @property
    def probability(self) -> float:
        """Get the event probability."""
        return self._probability

    @property
    def attributes(self) -> list:
        """Get the event attributes."""
        return self._attributes

class LogProcessor:

    def __init__(self, log_path, USE_MQTT, url, topic, port, user, pw):
        # Define whether log is read or just listening to
        if log_path:
            self.log = LogReader(log_path)
        else:
            self.log = []
        self.USE_MQTT = USE_MQTT
        # Setup queue for async processing
        self.message_queue = queue.Queue()

        # "SenderReceiver"
        if USE_MQTT == "SenderReceiver":
            # Sender
            self.sender = XESMQTTProducer(url, topic, port, user, pw)
            self.sender.connect()

            # Receiver
            self.receiver = XESMQTTConsumer(url, topic, port, user, pw)
            self.receiver.connect(True)
            self.receiver.subscribe_default(lambda payload: self.message_queue.put(payload))
            # Threading
            self.sender_thread = threading.Thread(target=self._send_messages, daemon=True)
            self.sender_thread.start()
        # "Receiver"
        elif USE_MQTT == "Receiver":
            # Receiver
            self.receiver = XESMQTTConsumer(url, topic, port, user, pw)
            self.receiver.connect(True)
            self.receiver.subscribe_default(lambda payload: self.message_queue.put(payload))
        # False
        else:
            # Threading
            self.sender_thread = threading.Thread(target=self._send_messages, daemon=True)
            self.sender_thread.start()

    def _send_messages(self):
        """Internal method to send messages in the background."""
        for event in self.log:
            if self.USE_MQTT == "SenderReceiver":
                self.sender.publish(event)
            else:
                event = {"name": event.name,
                                "timestamp": event.timestamp,
                                "probability": event.probability,
                                "attributes": event.attributes}
                self.message_queue.put(event)
            time.sleep(0.1)

        if self.USE_MQTT == "SenderReceiver":
            self.sender.disconnect()

            self.receiver.unsubscribe_default()
            self.receiver.disconnect()
