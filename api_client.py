import pandas as pd
import requests
from io import StringIO

def fetch_station_data(station, start_date="2020-01-01", end_date="2025-03-28", sensor_num=6, dur_code='D'):
    """
    Fetch reservoir data for a given station by calling the API.
    Returns a pandas DataFrame if successful, else None.
    """
    base_url = "https://cdec.water.ca.gov/dynamicapp/selectQuery"
    params = {
        "Stations": station,
        "SensorNums": sensor_num,
        "dur_code": dur_code,
        "Start": start_date,
        "End": end_date
    }
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        # Assume that the response is a CSV file.
        df = pd.read_csv(StringIO(response.text))
        return df
    except Exception as e:
        print(f"Error fetching data for station {station}: {e}")
        return None

def compute_stats(df):
    """
    Compute maximum, minimum and average water level from the DataFrame.
    Assumes that the water level values are in a column named 'Value'.
    """
    # Ensure 'Value' is numeric.
    df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
    df = df.dropna(subset=['Value'])

    if df.empty:
        return None

    stats = {
        "max_water_level": df['Value'].max(),
        "min_water_level": df['Value'].min(),
        "avg_water_level": df['Value'].mean()
    }
    return stats