import requests
import pandas as pd
import os
from io import StringIO

def import_data(url, team_member_id="M2"):
    
    try:
        response = requests.get(url, timeout=10)  
        response.raise_for_status()  
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download data: {e}")
    

    os.makedirs("data", exist_ok=True)
    

    file_path = os.path.join("data", f"dataset_{team_member_id}.txt")
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(response.text)
    
    
    df = pd.read_csv(StringIO(response.text)) 
    return df

def clean_data(df, team_member_id="M2"):
    

    if 'time' in df.columns:

        df['time'] = pd.to_datetime(df['time']).dt.strftime('%d-%m-%Y %H:%M:%S')
    else:
        raise ValueError("The 'time' column is missing from the dataset.")
    

    os.makedirs("output", exist_ok=True)
    

    output_path = os.path.join("output", f"cleaned_data_{team_member_id}.txt")
    df.to_csv(output_path, index=False, sep="\t") 
    
    return df


url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=2023-01-02&endtime=2023-01-03"
df = import_data(url, team_member_id="M2")
print("Original Data:")
print(df.head())

df_cleaned = clean_data(df, team_member_id="M2")
print("\nCleaned Data:")
print(df_cleaned.head())
print("\nData Info:")
print(df_cleaned.info())
