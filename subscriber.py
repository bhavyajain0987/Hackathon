import paho.mqtt.client as mqtt
import json
import pandas as pd

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[SUBSCRIBER] Connected to MQTT broker successfully!")
        client.subscribe("reservoir/+")
        print("[SUBSCRIBER] Subscribed to topic: reservoir/+")
    else:
        print(f"[SUBSCRIBER] Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        station = payload.get("station")
        data_str = payload.get("data")
        if not data_str:
            print(f"[SUBSCRIBER] No data in message for station {station}.")
            return

        # Convert the JSON string (list of records) into a DataFrame.
        records = json.loads(data_str)
        df = pd.DataFrame(records)

        # Ensure the VALUE field is numeric.
        df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')
        df = df.dropna(subset=['VALUE'])

        if not df.empty:
            max_val = df['VALUE'].max()
            min_val = df['VALUE'].min()
            avg_val = df['VALUE'].mean()
            print(f"\n[SUBSCRIBER] Aggregated data for station {station}:")
            print(f"    Max VALUE: {max_val}")
            print(f"    Min VALUE: {min_val}")
            print(f"    Avg VALUE: {avg_val}")
        else:
            print(f"\n[SUBSCRIBER] No valid VALUE entries for station {station}.")
    except Exception as e:
        print(f"[SUBSCRIBER] Error processing message from topic {msg.topic}: {e}")

def main():
    print("[SUBSCRIBER] Starting MQTT subscriber...")
    # Specify client_id and callback_api_version as keywords.
    client = mqtt.Client(client_id="ReservoirSubscriber", callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect("localhost", 1883, 60)
        client.loop_forever()
    except Exception as e:
        print(f"[SUBSCRIBER] Error connecting to MQTT broker: {e}")

if __name__ == "__main__":
    main()