import json
import time
import pandas as pd
from typing import Optional
import requests
from datetime import datetime

username = "TAKA00003"
password = "d47157548a35e8c1e27b9e61c54244999a1eef8a9b8b4a93f170ae4110677f81"
ctStatusUrl = "https://fleetapi-id.cartrack.com/rest/vehicles/status"
takariUrl = "http://traccar.haleyorapower.co.id:5055/"
dirData = ""

def getStatusData(registration: str):
    response = requests.get(ctStatusUrl, auth=(username, password), params={"registration": registration})
    if response.status_code == 200:
        print(f"HTTP request successful. Status code: {response.status_code}")
        print(f"Data fetched successfully for {registration}")
        return response.json()
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        print(f"Response content: {response.text}")
        return None
    
def getVehicleList() -> list:
    try:
        df = pd.read_excel(f"{dirData}")
        return df["registration"].tolist(), df["imei"].tolist()
    except Exception as e:
        print(f"Error reading vehicle list: {e}")
        return []

def reformatTimestamp(timestamp: Optional[str]) -> Optional[str]:
    if not timestamp:
        return None
    ts = timestamp.replace("Z","+00:00")
    try:
        return datetime.fromisoformat(ts).isoformat()
    except ValueError:
        return timestamp

def bodyBuilderTakari(vehicle: dict) -> dict:
    location = vehicle.get("location") or {}
    coords = {
        "latitude": vehicle.get("latitude"),
        "longitude": vehicle.get("longitude"),
        "accuracy": vehicle.get("gps_fix_type"),
        "speed": vehicle.get("speed"),
        "heading": vehicle.get("brearing"),
        "altitude": vehicle.get("altitude")
    }
    battery_pct = vehicle.get("tcu_percentage")
    battery_level = battery_pct
    event_ts = reformatTimestamp(vehicle.get("event_ts") or location.get("updated"))
    extras ={}

    body = {
        "location": {
            "timestamp": event_ts,
            "coords": coords,
            "is_moving": bool(vehicle.get("speed", 0)),
            "odometer": vehicle.get("odometer", 0),
            "event": (" "),
            "battery": {
                "level": battery_level,
                "is_charging": bool(vehicle.get("ignition")) if vehicle.get("ignition") is not None else False,
            },
            "activity": {"type": ""},
            "extras": {k: v for k, v in extras.items() if v not in (None, "", [])},
        },
        "device_id": 0,
    }

    return body

def sendToTakari(data: dict):
    print(f"Sending data to Takari: {json.dumps(data)}")
    print(f"URL: {takariUrl}")
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(takariUrl, headers=headers, data=json.dumps(data), timeout=30)
        if response.status_code == 200:
            print(f"Status Code: {response.status_code}. Data sent successfully to Takari")
        else:
            print(f"Failed to send data. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
    except Exception as e:
        print(f"Error sending data to Takari: {e}")

def main() -> None:
    try:
        vehiclesList = getVehicleList()
        for registration in vehiclesList[0] :
            print(f"Processing vehicle: {registration}")
            statusData = getStatusData(registration)
            if statusData and "vehicle" in statusData:
                body = bodyBuilderTakari(statusData["vehicle"])
                sendToTakari(body)
            else:
                print(f"No valid data for {registration}")
    except Exception as e:
        print(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()