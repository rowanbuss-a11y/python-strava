import os
import requests
import json
import datetime
import logging
from supabase import create_client, Client

# === Logging instellen ===
logging.basicConfig(level=logging.DEBUG, format="DEBUG: %(message)s")

# === Hulpfunctie om omgevingsvariabelen op te halen ===
def env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

# === Supabase client ===
def get_supabase() -> Client:
    supabase_url = env("SUPABASE_URL")
    supabase_key = env("SUPABASE_KEY")
    return create_client(supabase_url, supabase_key)

# === Functie: nieuw access token ophalen via refresh ===
def refresh_access_token() -> str:
    client_id = env("STRAVA_CLIENT_ID")
    client_secret = env("STRAVA_CLIENT_SECRET")
    refresh_token = env("STRAVA_REFRESH_TOKEN")

    response = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
    )

    if response.status_code != 200:
        raise RuntimeError(f"Strava token refresh failed: {response.text}")

    tokens = response.json()
    logging.debug("Nieuw access token verkregen")
    return tokens["access_token"]

# === Functie: activiteiten ophalen ===
def fetch_recent_activities(access_token: str, days: int = 30):
    after = int((datetime.datetime.now() - datetime.timedelta(days=days)).timestamp())
    page = 1
    activities = []

    while True:
        url = f"https://www.strava.com/api/v3/athlete/activities?after={after}&page={page}&per_page=200"
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
        if r.status_code != 200:
            r.raise_for_status()

        page_data = r.json()
        if not page_data:
            break

        logging.debug(f"Pagina {page} → {len(page_data)} activiteiten")
        activities.extend(page_data)
        page += 1

    logging.debug(f"Totaal {len(activities)} activiteiten opgehaald")
    return activities

# === Functie: upload naar Supabase ===
def upload_to_supabase(data):
    supabase = get_supabase()
    try:
        resp = supabase.table("strava_activities").upsert(data, on_conflict="id").execute()
        logging.debug(f"{len(data)} activiteiten geüpload naar Supabase")
        return resp
    except Exception as e:
        logging.error(f"Upload error: {e}")
        raise

# === Main ===
def main():
    logging.debug("Start sync - laatste 30 dagen")

    token = refresh_access_token()
    activities = fetch_recent_activities(token, days=30)

    # Alleen relevante velden + calorieën
    prepared = []
    for act in activities:
        prepared.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date"),
            "distance": act.get("distance"),
            "moving_time": act.get("moving_time"),
            "total_elevation_gain": act.get("total_elevation_gain"),
            "average_speed": act.get("average_speed"),
            "max_speed": act.get("max_speed"),
            "average_heartrate": act.get("average_heartrate"),
            "max_heartrate": act.get("max_heartrate"),
            "calories": act.get("calories"),  # ✅ calorieën toegevoegd
        })

    # Opslaan als backup (optioneel)
    with open("activiteiten_raw.json", "w") as f:
        json.dump(activities, f, indent=2)
        logging.debug(f"Saved {len(activities)} activities to activiteiten_raw.json")

    # Upload naar Supabase
    upload_to_supabase(prepared)
    logging.debug("Sync afgerond ✅")

# === Script uitvoeren ===
if __name__ == "__main__":
    main()
