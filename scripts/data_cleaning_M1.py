import requests
import pandas as pd
import os
from io import StringIO

def import_data(url, team_member_id="M1"):
    # Download the dataset
    try:
        response = requests.get(url, timeout=10)  # Add a timeout for robustness
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download data: {e}")
    
    # Ensure the 'data' folder exists
    os.makedirs("data", exist_ok=True)
    
    # Save the dataset as a .txt file
    file_path = os.path.join("data", f"dataset_{team_member_id}.txt")
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(response.text)
    
    # Load the dataset into a pandas DataFrame
    df = pd.read_csv(StringIO(response.text))  # Use StringIO to read the text directly
    return df

def clean_data(df, team_member_id="M1"):
    # Check if the 'time' column exists
    if 'time' in df.columns:
        # Convert the 'time' column to the desired format
        df['time'] = pd.to_datetime(df['time']).dt.strftime('%d-%m-%Y %H:%M:%S')
    else:
        raise ValueError("The 'time' column is missing from the dataset.")
    
    # Ensure the 'output' folder exists
    os.makedirs("output", exist_ok=True)
    
    # Save the cleaned data as a .txt file
    output_path = os.path.join("output", f"cleaned_data_{team_member_id}.txt")
    df.to_csv(output_path, index=False, sep="\t")  # Use tab separator for .txt file
    
    return df

# Example usage
url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=2023-01-01&endtime=2023-01-02"
df = import_data(url, team_member_id="M1")
print("Original Data:")
print(df.head())

df_cleaned = clean_data(df, team_member_id="M1")
print("\nCleaned Data:")
print(df_cleaned.head())
print("\nData Info:")
print(df_cleaned.info())
