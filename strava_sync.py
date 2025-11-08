"""
Strava sync script - laatste 30 dagen
Vereenvoudigde versie met directe Supabase upload
"""

import os
import csv
import json
import time
from datetime import datetime, timedelta
import requests

# ----------------------
# Environment variables
# ----------------------
STRAVA_CLIENT_ID = os.environ["STRAVA_CLIENT_ID"]
STRAVA_CLIENT_SECRET = os.environ["STRAVA_CLIENT_SECRET"]
STRAVA_REFRESH_TOKEN = os.environ.get("STRAVA_REFRESH_TOKEN")
STRAVA_AUTH_CODE = os.environ.get("STRAVA_AUTH_CODE")
STRAVA_REDIRECT_URI = os.environ.get("STRAVA_REDIRECT_URI", "")

CSV_FILE = os.environ.get("CSV_FILE", "activiteiten.csv")
JSON_FILE = os.environ.get("JSON_FILE", "activiteiten_raw.json")
DAYS_BACK = int(os.environ.get("DAYS_BACK", "30"))

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_TABLE = "strava_activities"

# ----------------------
# Helper functions
# ----------------------

def get_access_token():
    """Verkrijg access token van Strava"""
    if STRAVA_REFRESH_TOKEN:
        payload = {
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "refresh_token": STRAVA_REFRESH_TOKEN,
            "grant_type": "refresh_token",
        }
        response = requests.post("https://www.strava.com/oauth/token", data=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        print("DEBUG: Nieuw access token verkregen")
        return data["access_token"]
    elif STRAVA_AUTH_CODE:
        payload = {
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "code": STRAVA_AUTH_CODE,
            "grant_type": "authorization_code",
        }
        if STRAVA_REDIRECT_URI:
            payload["redirect_uri"] = STRAVA_REDIRECT_URI
        response = requests.post("https://www.strava.com/oauth/token", data=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        print("DEBUG: Access token via auth code verkregen")
        return data["access_token"]
    else:
        raise RuntimeError("Geen STRAVA_REFRESH_TOKEN of STRAVA_AUTH_CODE ingesteld")

def fetch_recent_activities(access_token: str, days: int = 30):
    """Haal activiteiten van de laatste N dagen"""
    activities = []
    page = 1
    per_page = 200
    after_ts = int((datetime.now() - timedelta(days=days)).timestamp())

    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"page": page, "per_page": per_page, "after": after_ts}
        response = requests.get(url, headers=headers, params=params, timeout=60)

        if response.status_code == 429:
            print("DEBUG: Rate limit - wachten 60s...")
            time.sleep(60)
            continue
        response.raise_for_status()
        page_acts = response.json()
        if not page_acts:
            break
        activities.extend(page_acts)
        print(f"DEBUG: Pagina {page} → {len(page_acts)} activiteiten")
        if len(page_acts) < per_page:
            break
        page += 1

    print(f"DEBUG: Totaal {len(activities)} activiteiten opgehaald")
    return activities

def save_to_csv(activities: list[dict], filename: str):
    """Opslaan in CSV"""
    if not activities:
        return
    fieldnames = [
        "ID", "Naam", "Datum", "Type", "Afstand (km)", "Tijd (min)",
        "Totale tijd (min)", "Hoogtemeters", "Gemiddelde snelheid (km/u)",
        "Max snelheid (km/u)", "Gemiddelde hartslag", "Max hartslag",
    ]
    existing_ids = set()
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_ids.add(row.get("ID"))

    rows = []
    for act in activities:
        act_id = str(act.get("id"))
        if act_id in existing_ids:
            continue
        rows.append({
            "ID": act_id,
            "Naam": act.get("name"),
            "Datum": act.get("start_date"),
            "Type": act.get("type"),
            "Afstand (km)": round(act.get("distance", 0)/1000, 2),
            "Tijd (min)": round(act.get("moving_time", 0)/60),
            "Totale tijd (min)": round(act.get("elapsed_time", 0)/60),
            "Hoogtemeters": act.get("total_elevation_gain", 0),
            "Gemiddelde snelheid (km/u)": round(act.get("average_speed", 0)*3.6,2),
            "Max snelheid (km/u)": round(act.get("max_speed",0)*3.6,2),
            "Gemiddelde hartslag": act.get("average_heartrate"),
            "Max hartslag": act.get("max_heartrate"),
        })

    mode = "a" if os.path.exists(filename) else "w"
    with open(filename, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
            writer.writeheader()
        writer.writerows(rows)
    print(f"DEBUG: {len(rows)} activiteiten opgeslagen in {filename}")

def save_to_json(activities: list[dict], filename: str):
    """Opslaan in JSON"""
    existing = []
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except:
                existing = []
    existing.extend(activities)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(existing, f)
    print(f"DEBUG: {len(activities)} activiteiten opgeslagen in {filename}")

def upload_to_supabase(activities: list[dict]):
    """Upload Strava activiteiten naar Supabase via REST API"""
    if not activities or not SUPABASE_URL or not SUPABASE_KEY:
        return

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    # Velden voorbereiden & float → int waar nodig
    for act in activities:
        if "moving_time" in act and isinstance(act["moving_time"], float):
            act["moving_time"] = int(act["moving_time"])
        if "elapsed_time" in act and isinstance(act["elapsed_time"], float):
            act["elapsed_time"] = int(act["elapsed_time"])
        if "distance" in act and isinstance(act["distance"], float):
            act["distance"] = float(act["distance"])

        start_latlng = act.get("start_latlng") or []
        end_latlng = act.get("end_latlng") or []
        act["start_latitude"] = start_latlng[0] if len(start_latlng)>0 else None
        act["start_longitude"] = start_latlng[1] if len(start_latlng)>1 else None
        act["end_latitude"] = end_latlng[0] if len(end_latlng)>0 else None
        act["end_longitude"] = end_latlng[1] if len(end_latlng)>1 else None

    response = requests.post(url, headers=headers, json=activities, timeout=30)
    if response.status_code in (200, 201):
        print(f"DEBUG: {len(activities)} activiteiten geüpload naar Supabase")
    else:
        print(f"DEBUG: Fout bij upload: {response.status_code} {response.text}")

# ----------------------
# Main
# ----------------------
def main():
    print(f"DEBUG: Start sync - laatste {DAYS_BACK} dagen")
    token = get_access_token()
    activities = fetch_recent_activities(token, DAYS_BACK)

    if not activities:
        print("DEBUG: Geen nieuwe activiteiten")
        return

    save_to_csv(activities, CSV_FILE)
    save_to_json(activities, JSON_FILE)
    upload_to_supabase(activities)
    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
