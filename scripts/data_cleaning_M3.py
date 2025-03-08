import requests
import pandas as pd
import os
from io import StringIO

def import_data(url, team_member_id="M3"):
    """
    Downloads the dataset from the given URL and saves it as 'dataset_M3.txt'.

    Parameters:
        url (str): The URL of the dataset.
        team_member_id (str): Identifier for team member (default "M3").

    Returns:
        pd.DataFrame: The raw dataset loaded into a Pandas DataFrame.
    """
    try:
        response = requests.get(url, timeout=10)  # Add timeout for robustness
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download data: {e}")
    
    # Ensure the 'data' folder exists
    os.makedirs("data", exist_ok=True)
    
    # Save dataset as a .txt file
    file_path = os.path.join("data", f"dataset_{team_member_id}.txt")
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(response.text)
    
    # Load dataset into a Pandas DataFrame
    df = pd.read_csv(StringIO(response.text))  # Read directly from response
    return df

def clean_data(df, team_member_id="M3"):
    """
    Cleans the dataset by converting the 'time' column to 'DD-MM-YYYY HH:MM:SS' format.

    Parameters:
        df (pd.DataFrame): The raw dataset.
        team_member_id (str): Identifier for team member (default "M3").

    Returns:
        pd.DataFrame: The cleaned dataset.
    """
    # Check if the 'time' column exists
    if 'time' in df.columns:
        # Convert time format
        df['time'] = pd.to_datetime(df['time'], errors="coerce").dt.strftime('%d-%m-%Y %H:%M:%S')
    else:
        raise ValueError("The 'time' column is missing from the dataset.")
    
    # Ensure the 'output' folder exists
    os.makedirs("output", exist_ok=True)
    
    # Save cleaned data as a .txt file (tab-separated for clarity)
    output_path = os.path.join("output", f"cleaned_data_{team_member_id}.txt")
    df.to_csv(output_path, index=False, sep="\t")  # Use tab separator for better readability
    
    return df

if __name__ == "__main__":
    # Define dataset URL
    dataset_url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=2023-01-01&endtime=2023-01-02"
    
    # Import dataset
    df_raw = import_data(dataset_url, team_member_id="M3")
    print("Original Data (First 5 Rows):")
    print(df_raw.head())  # Display first 5 rows of raw data

    # Clean dataset
    df_cleaned = clean_data(df_raw, team_member_id="M3")
    
    # Display cleaned data neatly
    print("Cleaned Data (First 10 Rows):")
    print(df_cleaned.head(10).to_string(index=False))  # Print first 10 rows

    # Show dataset structure
    print("Dataset Summary:")
    print(df_cleaned.info())
