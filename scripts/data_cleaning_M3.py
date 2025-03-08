import requests
import os
import pandas as pd
from io import StringIO
from datetime import datetime

def import_data(url):
    """
    Downloads the dataset from the provided URL and saves it as 'dataset_M3.txt'.

    Parameters:
        url (str): The URL of the dataset.

    Returns:
        list: A list of strings, each representing a row of the dataset.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses (e.g., 404, 500)

        # Ensure the 'data' directory exists
        os.makedirs("data", exist_ok=True)

        # Save the dataset
        file_path = "data/dataset_M3.txt"
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(response.text)

        return response.text.splitlines()

    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        return []

def clean_data(data_text_list):
    """
    Cleans the dataset by converting date formats to 'DD-MM-YYYY HH:MM:SS'.

    Parameters:
        data_text_list (list): A list of strings representing the raw dataset.

    Returns:
        list: A list of cleaned strings.
    """
    try:
        if not data_text_list:
            print("No data available for cleaning.")
            return []

        # Convert list of strings to DataFrame
        data_str = "\n".join(data_text_list)
        df = pd.read_csv(StringIO(data_str))

        # Check if the 'time' column exists and convert format
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.strftime("%d-%m-%Y %H:%M:%S")
        else:
            print("Warning: 'time' column not found in the dataset.")

        # Ensure the 'output' directory exists
        os.makedirs("output", exist_ok=True)

        # Save cleaned data
        output_file = "output/cleaned_data_M3.txt"
        df.to_csv(output_file, index=False)

        return df.to_csv(index=False).splitlines()

    except Exception as e:
        print(f"Error processing data: {e}")
        return []

if __name__ == "__main__":
    # Dataset URL
    dataset_url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=2023-01-01&endtime=2023-01-02"

    # Execute functions
    raw_data = import_data(dataset_url)
    cleaned_data = clean_data(raw_data)

    # Print confirmation
    if cleaned_data:
        print("Data cleaning completed successfully!")
    else:
        print("Data cleaning failed.")
