from api_client import fetch_station_data, compute_stats
from mqtt_publisher import MQTTPublisher

def main():
    # List of station IDs to process.
    stations = ["SHA", "STN2", "STN3"]  # Update with the actual station codes.

    publisher = MQTTPublisher(broker="localhost", port=1883)

    for station in stations:
        print(f"\nProcessing data for station: {station}")
        df = fetch_station_data(station)
        if df is None or df.empty:
            print(f"No data available for station {station}.")
            continue

        stats = compute_stats(df)
        if stats is None:
            print(f"Could not compute stats for station {station}.")
            continue

        publisher.publish(station, stats)

    publisher.disconnect()

if __name__ == "__main__":
    main()