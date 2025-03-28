import paho.mqtt.client as mqtt
import json
import time

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected successfully!")
    else:
        print("MQTT connection failed with code", rc)

class MQTTPublisher:
    def __init__(self, broker="localhost", port=1883):
        # Removed callback_api_version to avoid API version issues.
        self.client = mqtt.Client(client_id="ReservoirPublisher")
        self.client.on_connect = on_connect
        try:
            self.client.connect(broker, port, 60)
            self.client.loop_start()
        except Exception as e:
            print("Error connecting to MQTT broker:", e)

    def publish(self, station, data):
        """
        Publish the data (JSON string containing STATION_ID and VALUE fields)
        to the topic for the given station.
        """
        topic = f"reservoir/{station}"
        payload = {
            "station": station,
            "data": data
        }
        message = json.dumps(payload)
        result = self.client.publish(topic, message)
        status = result[0]
        if status == 0:
            print(f"Published data to topic '{topic}': {message}")
        else:
            print(f"Failed to send message to topic {topic}")
        time.sleep(1)

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()