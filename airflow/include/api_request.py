import requests
import pandas as pd

# function to fetch device_ids from the API 
def fetch_device_ids(url):
    try:
        # Send a GET request to the API
        response = requests.get(url)
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        # Parse the JSON response
        data = response.json()
        
        # Use a set to store unique device IDs
        device_ids = {feed["device_id"] for feed in data.get("feeds", [])}

        return list(device_ids)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return []

# function to fetch pm25 information from api
def get_last_7_days_pm25(device_id):
    try:
        # Define the URL for the device history endpoint
        api_url = f"https://pm25.lass-net.org/API-1.0.0/device/{device_id}/history/?format=JSON"
        # Send a GET request to the API
        response = requests.get(api_url)
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        # Parse the JSON response
        data = response.json()

        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None