"""
Angelina County Parcel & Abstract Data Downloader
===================================================
Run this script to download parcel boundaries and abstract boundaries
from the Angelina County Appraisal District's ArcGIS service.

Usage:
    python download_data.py

This creates two files in the current directory:
    - angelina_parcels.geojson  (~40 MB, 60,763 parcels with owner info)
    - angelina_abstracts.geojson (~1 MB, 1,013 abstract boundaries)
"""

import json
import urllib.request
import urllib.parse
import os
import sys
import time

BASE_URL = "https://utility.arcgis.com/usrsvcs/servers/0d57665b0361492397b48cbd4ad88ad6/rest/services/AngelinaCADWebService/FeatureServer"

PARCEL_FIELDS = "prop_id,file_as_name,legal_acreage,legal_desc,abs_subdv_cd,land_val,market,situs_street,situs_city,geo_id,Deed_Date"
ABSTRACT_FIELDS = "CODE,DESC_,Block,Surv_Sect,Surv_Name"

BATCH_SIZE = 2000

def fetch_json(url):
    """Fetch JSON from URL with retry logic."""
    for attempt in range(3):
        try:
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'Mozilla/5.0')
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            if attempt < 2:
                print(f"  Retry {attempt + 1}/3: {e}")
                time.sleep(2)
            else:
                raise

def get_count(layer_id):
    """Get total feature count for a layer."""
    url = f"{BASE_URL}/{layer_id}/query?where=1%3D1&returnCountOnly=true&f=json"
    data = fetch_json(url)
    return data.get('count', 0)

def download_layer(layer_id, fields, name):
    """Download all features from a layer in batches."""
    total = get_count(layer_id)
    print(f"\n{'='*50}")
    print(f"Downloading {name}: {total:,} features")
    print(f"{'='*50}")

    all_features = []
    offset = 0
    batch_num = 0

    while offset < total:
        batch_num += 1
        params = urllib.parse.urlencode({
            'where': '1=1',
            'outFields': fields,
            'outSR': '4326',
            'f': 'geojson',
            'resultRecordCount': BATCH_SIZE,
            'resultOffset': offset
        })
        url = f"{BASE_URL}/{layer_id}/query?{params}"

        data = fetch_json(url)
        features = data.get('features', [])

        if not features:
            break

        # Reduce coordinate precision to 5 decimals (~1m accuracy)
        for f in features:
            if f.get('geometry') and f['geometry'].get('coordinates'):
                f['geometry']['coordinates'] = round_coords(f['geometry']['coordinates'])

        all_features.extend(features)
        offset += BATCH_SIZE

        pct = min(100, int(len(all_features) / total * 100))
        print(f"  Batch {batch_num}: {len(all_features):,} / {total:,} ({pct}%)")

    geojson = {
        "type": "FeatureCollection",
        "features": all_features
    }

    print(f"  Total downloaded: {len(all_features):,} features")
    return geojson

def round_coords(coords):
    """Recursively round coordinates to 5 decimal places."""
    if isinstance(coords, (int, float)):
        return round(coords, 5)
    return [round_coords(c) for c in coords]

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    print("Angelina County Data Downloader")
    print("Source: Angelina CAD ArcGIS FeatureServer")
    print(f"Output directory: {script_dir}")

    # Download abstracts (small, single batch)
    abstracts = download_layer(1, ABSTRACT_FIELDS, "Abstracts")
    abs_path = os.path.join(script_dir, "angelina_abstracts.geojson")
    with open(abs_path, 'w') as f:
        json.dump(abstracts, f)
    size_mb = os.path.getsize(abs_path) / 1024 / 1024
    print(f"  Saved: {abs_path} ({size_mb:.1f} MB)")

    # Download parcels (large, many batches)
    parcels = download_layer(0, PARCEL_FIELDS, "Parcels")
    parcel_path = os.path.join(script_dir, "angelina_parcels.geojson")
    with open(parcel_path, 'w') as f:
        json.dump(parcels, f)
    size_mb = os.path.getsize(parcel_path) / 1024 / 1024
    print(f"  Saved: {parcel_path} ({size_mb:.1f} MB)")

    print(f"\n{'='*50}")
    print("DONE! Both files saved.")
    print(f"{'='*50}")
    print("\nNext steps:")
    print("  1. Open a terminal in this folder")
    print("  2. Run: python -m http.server 8080")
    print("  3. Open: http://localhost:8080/angelina_lease_tracker.html")

if __name__ == '__main__':
    main()
