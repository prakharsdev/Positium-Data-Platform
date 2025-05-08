import os
import requests
from dotenv import load_dotenv

# Load .env variables if needed (not strictly necessary here)
load_dotenv()

# URL and local path
GEOJSON_URL = "https://gisco-services.ec.europa.eu/distribution/v2/lau/geojson/LAU_RG_01M_2021_3035.geojson"
OUTPUT_DIR = "data/raw/lau_boundaries"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "LAU_RG_01M_2021_3035.geojson")

def download_geojson():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Downloading LAU GeoJSON from {GEOJSON_URL}...")
    response = requests.get(GEOJSON_URL)
    
    if response.status_code == 200:
        with open(OUTPUT_FILE, "wb") as f:
            f.write(response.content)
        print(f"GeoJSON file saved to: {OUTPUT_FILE}")
    else:
        print(f"Failed to download GeoJSON. Status code: {response.status_code}")

if __name__ == "__main__":
    download_geojson()
