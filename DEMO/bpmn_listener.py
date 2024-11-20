import json
import paho.mqtt.client as mqtt


class base_mqtt:

    def __init__(self):
        # MQTT connection and client
        broker_address = "ftsim.weber.ics.unisg.ch"
        port = 1883
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.username_pw_set("ftsim", "unisg")
        self.client.connect(broker_address, port)
        self.lastLeft = "L0"
        self.lastRight = "R0"
        self.client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.client.subscribe("smart-healthcare/DEMO")
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    def on_message(self, client, userdata, msg):
        payload = json.loads(msg.payload.decode("utf-8"))
        name = payload[0]
        side = payload[1]["patient"]
        mapping_activities = {
                "Patient Left":
                    {
                    "Donor check-in": "L1",
                    "HCW check-in": "L2",
                    "Perform Hand hygiene": "L3",
                    "Check blood drawing machine": "L4",
                    "Apply tourniquet": "L5",
                    "Disinfect injection site": "L7",
                    "Insert needle": "L8",
                    "Remove tourniquet": "L9",
                    "Start blood drawing machine": "L10",
                    "Monitor patient": "L11",
                    "Stop blood drawing machine": "L12",
                    "Remove needle": "L13",
                    "Donor check-out": "L15",
                    "Take out samples": "L16",
                    "HCW check-out": "L18",
                    },
                "Patient Right":
                    {
                    "Donor check-in": "R1",
                    "HCW check-in": "R2",
                    "Perform Hand hygiene": "R3",
                    "Check blood drawing machine": "R4",
                    "Apply tourniquet": "R5",
                    "Disinfect injection site": "R7",
                    "Insert needle": "R8",
                    "Remove tourniquet": "R9",
                    "Start blood drawing machine": "R10",
                    "Monitor patient": "R11",
                    "Stop blood drawing machine": "R12",
                    "Remove needle": "R13",
                    "Donor check-out": "R15",
                    "Take out samples": "R16",
                    "HCW check-out": "R18",
                    }
            }
        current_event = mapping_activities[side][name]
        to_send = None
        if current_event[0] == "L":
            if int(current_event[1:]) == 3:
                if int(self.lastLeft[1:]) == 2:
                    to_send = "L3"
                elif int(self.lastLeft[1:]) == 5:
                    to_send = "L6"
                elif int(self.lastLeft[1:]) == 13:
                    to_send = "L14"
                elif int(self.lastLeft[1:]) == 16:
                    to_send = "L17"
            else:
                to_send = current_event
                self.lastLeft = current_event
        else:
            if int(current_event[1:]) == 3:
                if int(self.lastRight[1:]) == 2:
                    to_send = "R3"
                elif int(self.lastRight[1:]) == 5:
                    to_send = "R6"
                elif int(self.lastRight[1:]) == 13:
                    to_send = "R14"
                elif int(self.lastRight[1:]) == 16:
                    to_send = "R17"
            else:
                to_send = current_event
                self.lastRight = current_event
        topic = "smart-healthcare/DEMX"
        payload = {to_send: 1}
        if to_send:
            self.client.publish(topic, json.dumps(payload))

base_mqtt()
