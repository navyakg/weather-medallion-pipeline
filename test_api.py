import json
import os 
from datetime import datetime
import requests 

CITIES = {
    "Bangalore": {"lat": 12.9716, "lon": 77.5946},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Delhi": {"lat": 28.6139, "lon": 77.2090},
}

def fetch_weather():
    timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")

    for city_name, coords in CITIES.items():
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={coords['lat']}&longitude={coords['lon']}&"
            f"current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,wind_speed_10m"
        )

        response = requests.get(url)

        if response.status_code==200:
            data=response.json()
            data["city_name"]=city_name
            data["extracted_at"]=datetime.now().isoformat()

            output_dir="D:/data-engineering-pipeline/data/bronze"
            os.makedirs(output_dir, exist_ok=True)

            filename=f"{output_dir}/{city_name.lower()}_{timestamp}.json"

            with open(filename, "w") as f:
                json.dump(data,f,indent=4)

            print(f" Successfully saved raw data for {city_name} -> {filename}")
        else:
            print(f"❌ Failed to fetch data for {city_name}: Status {response.status_code}")



if __name__=="__main__":
    fetch_weather()