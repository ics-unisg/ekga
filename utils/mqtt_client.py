################################################################################################
#                                                                                                                                                                                            #
# Copied and adapted from Aaron's code                                                                                                                             #
#                                                                                                                                                                                            #
################################################################################################
from abc import ABC
from typing import Optional, Callable, Any
import validators
import uuid
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.client import Client, MQTTMessage

import json
import time


class _XESMQTTClient(ABC):
    """Abstract client serving as common basis for consumers or producers clients."""

    def __init__(self, broker_host: str, topic_base: str, mqtt_port: int = 8883,
                        mqtt_username: Optional[str] = None, mqtt_pw: Optional[str] = None) -> None:
        """Initialize the client."""

        if not validators.domain(broker_host):
            raise ValueError("Invalid broker host: Please enter a valid domain")
        self._broker_host = broker_host
        self._topic_base = topic_base
        self._mqtt_port = mqtt_port

        self._client_id = uuid.uuid4().hex
        self._client = mqtt.Client(CallbackAPIVersion.VERSION2, self._client_id)

        if mqtt_username and mqtt_pw:
            self._client.username_pw_set(mqtt_username, mqtt_pw)
        self._client_is_connected = False

        self._client.on_connect = lambda client, userdata, connect_flags, reason_code, properties: (
                print(f"INIT - {reason_code.getId(reason_code.getName())}: {reason_code.getName()}"))

    @property
    def broker_host(self) -> str:
        """Get the broker host."""
        return self._broker_host

    @property
    def topic_base(self) -> str:
        """Get the topic base."""
        return self._topic_base

    @property
    def mqtt_port(self) -> int:
        """Get the MQTT port."""
        return self._mqtt_port

    @property
    def id(self) -> str:
        """Get the client id."""
        return self._client_id

    @property
    def is_connected(self) -> bool:
        """Get the connection status."""
        return self._client_is_connected

    def connect(self) -> None:
        """Connect to the MQTT broker."""
        if not self._client_is_connected:
            self._client.connect(self._broker_host, self._mqtt_port)
            self._client_is_connected = True
        else:
            raise AlreadyConnectedException("Client is already connected")

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        if self._client_is_connected:
            self._client.disconnect()
            self._client_is_connected = False
        else:
            raise NotConnectedException("Client is not connected")

class XESMQTTProducer(_XESMQTTClient):
    """Class for producing XES messages to MQTT."""

    def publish(self, event, qos: int = 2) -> bool:
        """Publish an XES event to the MQTT broker."""
        if not self._client_is_connected:
            raise NotConnectedException("Client is not connected")

        topic = f"{self._topic_base}"
        msg_info = self._client.publish(topic, json.dumps({"name": event.name,
                                                                                          "timestamp": event.timestamp,
                                                                                          "probability": event.probability,
                                                                                          "attributes": event.attributes}), qos=qos)
        while not msg_info.is_published():
            self._client.loop()
            time.sleep(0.1)
        return msg_info.is_published()

class XESMQTTConsumer(_XESMQTTClient):
    """Class for consuming XES messages on MQTT."""

    def __init__(self, broker_host: str, topic_base: str, mqtt_port: int = 8883,
                        mqtt_username: Optional[str] = None, mqtt_pw: Optional[str] = None) -> None:
        """Initialize the client."""
        super().__init__(broker_host, topic_base, mqtt_port, mqtt_username, mqtt_pw)

        self._threaded = True
        self._subscribed_base_topic = False

    def connect(self, threaded: bool = True) -> None:
        """Connect to the MQTT broker.

        Overrides the connect method of the parent class, to enable threaded message listening
        after connecting. Thread is stopped when calling disconnect.
        :param threaded: If True, the client will loop when subscribing in main thread and block."""
        super().connect()
        self._threaded = threaded
        if self._client_is_connected and threaded:
            self._client.loop_start()

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        super().disconnect()
        if self._threaded and not self._client_is_connected:
            self._client.loop_stop()

    def loop(self) -> None:
        """Execute a single loop iteration. Only for non-threaded connection."""
        if not self._client_is_connected:
            raise NotConnectedException("Client is not connected")
        if self._threaded:
            raise ConnectionTypeException("This method is only for non-threaded connections")
        self._client.loop()

    def subscribe_default(self, callback: Callable[[dict], None]) -> None:
        """Subscribe to the base topic on the MQTT broker.

        This callback will be used when there is a message received on the topic base, without any
        process name, case id or activity name.
        :param callback: The callback to be called when a message is received.
        """
        if not self._client_is_connected:
            raise NotConnectedException("Client is not connected")
        if self._client.on_message:
            raise AlreadySuchSubscriptionException("Default subscription already set")

        def callback_paho(_client: Client, _userdata: Any, message: MQTTMessage) -> None:
            try:
                payload_data = json.loads(message.payload.decode('utf-8'))
                callback(payload_data)
            except json.JSONDecodeError:
                print("Failed to decode JSON payload.")

        self._client.on_message = callback_paho
        if not self._subscribed_base_topic:
            if not self._client_is_connected:
                raise NotConnectedException("Client is not connected")
            self._client.subscribe(self._topic_base + "/#")
            self._subscribed_base_topic = True

    def unsubscribe_default(self) -> None:
        """Unsubscribe from the default topic base on the MQTT broker.

        This will remove the default callback for the topic base."""
        if not self._client_is_connected:
            raise NotConnectedException("Client is not connected")
        if not self._client.on_message:
            raise NoSuchSubscriptionException("No default subscription to unsubscribe from")
        self._client.on_message = None
        self._client.unsubscribe(self._topic_base + "/#")
        self._subscribed_base_topic = False


class Error(Exception):
    """Base class for all of this library's exceptions."""

class AlreadyConnectedException(Error):
    """The client is already connected."""

class NotConnectedException(Error):
    """The client is not connected."""

class ConnectionTypeException(Error):
    """Functionality only supported for a specific connection type (threaded/non-threaded)."""

class AlreadySuchSubscriptionException(Error):
    """A subscription with the same topic is already set.

    This exception is raised when trying to set a subscription with a topic that is already set.
    To change the subscription, first remove the existing subscription and then set the new one.
    """

class NoSuchSubscriptionException(Error):
    """No subscription with the given topic is set.

    This exception is raised when trying to remove a subscription with a topic that is not set.
    """
