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
        self.client = mqtt.Client("ReservoirPublisher")
        self.client.on_connect = on_connect
        try:
            self.client.connect(broker, port, 60)
            self.client.loop_start()
        except Exception as e:
            print("Error connecting to MQTT broker:", e)

    def publish(self, station, stats):
        topic = f"reservoir/{station}"
        payload = {
            "station": station,
            "max_water_level": stats.get("max_water_level"),
            "min_water_level": stats.get("min_water_level"),
            "avg_water_level": stats.get("avg_water_level")
        }
        message = json.dumps(payload)
        result = self.client.publish(topic, message)
        status = result[0]
        if status == 0:
            print(f"Published data to topic '{topic}': {message}")
        else:
            print(f"Failed to send message to topic {topic}")
        # Pause briefly between messages.
        time.sleep(1)

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()