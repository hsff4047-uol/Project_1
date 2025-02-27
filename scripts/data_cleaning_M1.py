import requests
import os

url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=2023-01-01&endtime=2023-01-02"

def import_data(url):
    response = requests.get(url)

    if response.status_code == 200:
        text_data = response.text
    else:
      raise Exception("Downloading data error")

    # Checks if the 'data' folder exists
    if not os.path.exists("data"):
      os.makedirs("data")

    # This joins my 'data' folder with my dataset file "dataset_M1.txt"
    file_path = os.path.join("data", "dataset_M1.txt")

    # Saving the downloaded data
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(text_data)

    with open(file_path, "r", encoding="utf-8") as f:
        data_lines = f.readlines()

    return data_lines

import_data(url)

