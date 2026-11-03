import json
from datetime import datetime, timedelta
from pathlib import Path

import requests


# 1. Cities
cities = [
    {"name": "Kyiv", "latitude": 50.4501, "longitude": 30.5234},
    {"name": "Lviv", "latitude": 49.8397, "longitude": 24.0297},
    {"name": "Warsaw", "latitude": 52.2297, "longitude": 21.0122},
]


# 2. Date range
end_date = datetime.today().date() - timedelta(days=7)
start_date = end_date - timedelta(days=29)


# 3. API settings
url = "https://archive-api.open-meteo.com/v1/archive"

params_template = {
    "daily": [
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
    ],
    "timezone": "auto",
}


# 4. Output directory
output_dir = Path(__file__).resolve().parent.parent / "data" / "raw" / "weather"
output_dir.mkdir(parents=True, exist_ok=True)


# 5. Request data for each city
failed = []

for city in cities:
    params = {
        **params_template,
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": str(start_date),
        "end_date": str(end_date),
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        weather_data = response.json()

        # Add city name
        weather_data["city"] = city["name"]

        # Save JSON
        output_path = output_dir / f"{city['name'].lower()}.json"
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(weather_data, file, ensure_ascii=False, indent=4)

        print(f"Saved: {output_path}")

    except requests.exceptions.RequestException as e:
        # network errors, timeout, HTTP 4xx/5xx
        print(f"Failed for {city['name']}: {e}")
        failed.append(city["name"])
    except (ValueError, OSError) as e:
        # invalid JSON in the response or file write error
        print(f"Error processing {city['name']}: {e}")
        failed.append(city["name"])


# 6. Summary
if failed:
    print(f"\nFailed to download: {', '.join(failed)}")
else:
    print("\nAll cities downloaded successfully")