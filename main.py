from api_client import fetch_station_data
from mqtt_publisher import MQTTPublisher

def main():
    # List of station IDs to process.
    stations = ["SHA", "ORO", "CLE", "NML", "SNL", "DNP", "BER", "FOL", "BUL", "PNF"]

    publisher = MQTTPublisher(broker="localhost", port=1883)

    for station in stations:
        print(f"\n[API] Processing data for station: {station}")
        df = fetch_station_data(station)
        if df is None or df.empty:
            print(f"[API] No data available for station {station}.")
            continue

        if "STATION_ID" in df.columns and "VALUE" in df.columns:
            filtered_df = df[['STATION_ID', 'VALUE']]
        else:
            print(f"[API] Columns 'STATION_ID' or 'VALUE' not found for station {station}.")
            continue

        # Convert the filtered DataFrame to a JSON string representing a list of records.
        data_json = filtered_df.to_json(orient="records")
        print(f"[MQTT] Publishing data for station {station} ...")
        publisher.publish(station, data_json)

    publisher.disconnect()
    print("\n[DONE] Finished publishing data for all stations.")

if __name__ == "__main__":
    main()