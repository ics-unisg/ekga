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

        # Handle :ns0 <--------------------------------------------------------------------------------------
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

        self.to_return = []
        for unparsed_event in self.events:
            event_keys = list(unparsed_event.keys())

            name = unparsed_event["concept:name"]
            event_keys.remove("concept:name")
            timestamp = unparsed_event["time:timestamp"]
            event_keys.remove("time:timestamp")
            probability = 1.0
            attributes = []

            if "uncertainty:discrete_weak" in event_keys:
                event_keys.remove("uncertainty:discrete_weak")
                uncertain_box = unparsed_event["uncertainty:discrete_weak"]["uncertainty:entries"]
                indeterminancy = False
                for uncertain_dict in uncertain_box:
                    uncertain_dict_keys = list(uncertain_dict.keys())
                    if "uncertainty:indeterminacy" in uncertain_dict:
                        if indeterminancy:
                            print(unparsed_event)
                            raise ValueError("Cannot have multiple indeterminancies in one uncertainty box!")
                        indeterminancy = True
                        probability = uncertain_dict["uncertainty:probability"]
                        uncertain_dict_keys.remove("uncertainty:probability")
                    else:
                        attribute_probability = float(uncertain_dict["uncertainty:probability"])
                        uncertain_dict_keys.remove("uncertainty:probability")
                        first_attributes = uncertain_dict_keys[0].split(":")
                        if len(first_attributes) != 2:
                            print(unparsed_event)
                            raise ValueError("Attribute name cannot contain : !")
                        attributes.append((first_attributes[0], first_attributes[1], uncertain_dict[uncertain_dict_keys[0]], attribute_probability))

            for key in event_keys:
                first_attributes = key.split(":")
                if len(first_attributes) != 2:
                    print(unparsed_event)
                    raise ValueError("Attribute name cannot contain : !")
                attributes.append((first_attributes[0], first_attributes[1], unparsed_event[key], 1.0))

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

    def __init__(self, log_path, USE_MQTT):
        if log_path:
            self.log = LogReader(log_path)
        else:
            self.log = []
        self.USE_MQTT = USE_MQTT
        self.message_queue = queue.Queue()

        # "SenderReceiver"
        if USE_MQTT == "SenderReceiver":
            self.sender = XESMQTTProducer("ftsim.weber.ics.unisg.ch", "smart-healthcare", 1883, "ftsim", "unisg")
            self.sender.connect()

            self.receiver = XESMQTTConsumer("ftsim.weber.ics.unisg.ch", "smart-healthcare", 1883, "ftsim", "unisg")
            self.receiver.connect(True)
            self.receiver.subscribe_default(lambda payload: self.message_queue.put(payload))

            self.sender_thread = threading.Thread(target=self._send_messages, daemon=True)
            self.sender_thread.start()
        # "Receiver"
        elif USE_MQTT == "Receiver":
            self.receiver = XESMQTTConsumer("ftsim.weber.ics.unisg.ch", "smart-healthcare", 1883, "ftsim", "unisg")
            self.receiver.connect(True)
            self.receiver.subscribe_default(lambda payload: self.message_queue.put(payload))
        # False
        else:
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
