import json
import time
import requests
import pandas as pd
import logging
import threading
from typing import Optional
from datetime import datetime, timezone

username = "TAKA00003"
password = "d47157548a35e8c1e27b9e61c54244999a1eef8a9b8b4a93f170ae4110677f81"
ctStatusUrl = "https://fleetapi-id.cartrack.com/rest/vehicles/status"
takariUrl = "http://traccar.haleyorapower.co.id:5059/post_position"
takariAuth = "SG9tZUZ1bGxzdGFjazpVcjFwTUBtcDFyTmdPbWIz"
dirData = "20260320-TAKA00003-IMEI.xlsx"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [TAKARI-INTEGRATION] - %(message)s",
    handlers=[
        logging.FileHandler('sendData.log'),  # Log to file
        logging.StreamHandler()  # Also print to console
    ]
)
logger = logging.getLogger(__name__)

def getStatusData(registration: str):
    response = requests.get(ctStatusUrl, auth=(username, password), params={"filter[registration]": registration})
    logger.info(f"Full URL Cartrack = {response.url}")
    if response.status_code == 200:
        logger.info(f"HTTP request successful. Status code: {response.status_code}")
        logger.info(f"Response Data : {response.text}")
        logger.info(f"Data fetched successfully for {registration}")
        return response.json()
    else:
        logger.error(f"Failed to fetch data. Status code: {response.status_code}")
        logger.error(f"Response content: {response.text}")
        return None
    
def getVehicleList() -> list:
    try:
        df = pd.read_excel(f"{dirData}")
        return df["Registration"].tolist(), df["IMEI"].tolist()
    except Exception as e:
        logger.error(f"Error reading vehicle list: {e}")
        return []

def reformatTimestamp(timestamp: Optional[str]) -> Optional[str]:
    if not timestamp:
        return None
    ts = timestamp.replace("Z","+00:00")
    try:
        dt = datetime.fromisoformat(ts)
        # Convert to UTC if it has timezone info
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc)
        else:
            # Assume UTC if no timezone info
            dt = dt.replace(tzinfo=timezone.utc)
        # Format as ISO 8601 with milliseconds and Z suffix
        return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    except ValueError:
        logger.warning(f"Failed to parse timestamp: {timestamp}")
        return timestamp

def bodyBuilderTakari(vehicle: dict, imei: str) -> dict:
    location = vehicle.get("location") or {}
    coords = {
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude"),
        "accuracy": location.get("gps_fix_type"),
        "speed": vehicle.get("speed"),
        "heading": vehicle.get("bearing"),
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
        "device_id": f"{imei}"
    }

    return body

def sendToTakari(data: dict):
    logger.info(f"Sending Data to Takari: {json.dumps(data)}")
    logger.info(f"URL: {takariUrl}")
    headers = {"Content-Type": "application/json",
               "Authorization": f"Basic {takariAuth}"}
    try:
        response = requests.post(takariUrl, headers=headers, data=json.dumps(data), timeout=30)
        if response.status_code == 200:
             logger.info(f"Status Code: {response.status_code}. Data sent successfully to Takari")
        else:
            logger.error(f"Failed to send data. Status code: {response.status_code}")
            logger.error(f"Response content: {response.text}")
    except Exception as e:
        logger.error(f"Error sending data to Takari: {e}")

def main() -> None:
    try:
        logger.info("Start Service")
        registrations, imeis = getVehicleList()
        for registration, imei in zip(registrations, imeis):
            logger.info(f"Processing vehicle: {registration} with IMEI: {imei}")
            statusData = getStatusData(registration)
            if statusData and "data" in statusData and len(statusData["data"]) > 0:
                vehicle = statusData["data"][0]
                body = bodyBuilderTakari(vehicle, imei)
                sendToTakari(body)
            else:
                logger.warning(f"No valid data for {registration}")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")

def scheduler():
    startTime = datetime.now()
    time.sleep(5)

    while True:
        endTime = datetime.now()

        startTimeStr = startTime.strftime("%Y-%m-%d %H:%M:%S")
        endTimeStr = endTime.strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"Running scheduled task - Start: {startTimeStr}, End: {endTimeStr}")
        
        try:
            main()
            startTime = endTime
        except Exception as e:
            logger.error(f"Error in scheduled task: {e}")

        time.sleep(60)  # Run every 60 seconds


if __name__ == "__main__":
    logger.info("Server started. Press Ctrl+C to stop.")
    thread = threading.Thread(target=scheduler)
    thread.daemon = True
    thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Server stopped.")