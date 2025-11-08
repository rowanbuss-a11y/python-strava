import requests
import json
import csv
import logging
import datetime
from supabase import create_client, Client

# ======================
# CONFIGURATIE
# ======================
STRAVA_CLIENT_ID = "129018"
STRAVA_CLIENT_SECRET = "69d0ce2fdd3cdfc33b037b5e43d3f9f3faf0eed4"
STRAVA_REFRESH_TOKEN = "aec3efbf4e76dcae6ec1c658c14e8620e5bfef5b"

SUPABASE_URL = "https://<JOUW_URL>.supabase.co"
SUPABASE_KEY = "<JOUW_SERVICE_ROLE_KEY>"
SUPABASE_TABLE = "strava_activities"

CSV_PATH = "/Users/rowanbuss/Desktop/STRAVA NIEUW/activiteiten.csv"
RAW_PATH = "/Users/rowanbuss/Desktop/STRAVA NIEUW/activiteiten_raw.json"

# Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ======================
# FUNCTIES
# ======================

def get_access_token():
    logging.debug("Nieuw access token aanvragen...")
    response = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": STRAVA_REFRESH_TOKEN,
        },
    )
    response.raise_for_status()
    data = response.json()
    access_token = data["access_token"]
    logging.debug("Nieuw access token verkregen")
    return access_token


def fetch_recent_activities(access_token, days=30):
    logging.debug("Fetching activities from last 30 days")
    after = int((datetime.datetime.now() - datetime.timedelta(days=days)).timestamp())
    all_activities = []
    page = 1

    while True:
        url = f"https://www.strava.com/api/v3/athlete/activities?after={after}&page={page}&per_page=200"
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
        if r.status_code != 200:
            r.raise_for_status()

        activities = r.json()
        if not activities:
            break

        logging.debug(f"Pagina {page} → {len(activities)} activiteiten")
        all_activities.extend(activities)
        page += 1

    logging.debug(f"Totaal {len(all_activities)} activiteiten opgehaald")
    return all_activities


def save_to_files(activities):
    logging.debug(f"Saving {len(activities)} activities to {CSV_PATH} and {RAW_PATH}")

    with open(RAW_PATH, "w") as f:
        json.dump(activities, f, indent=2)

    keys = ["id", "name", "type", "start_date", "distance", "moving_time", "average_speed", "calories"]
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for act in activities:
            writer.writerow({k: act.get(k, "") for k in keys})


def prepare_activities(activities):
    prepared = []
    for act in activities:
        prepared.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date"),
            "distance": act.get("distance"),
            "moving_time": act.get("moving_time"),
            "average_speed": act.get("average_speed"),
            "calories": act.get("calories"),
        })
    return prepared


def upload_to_supabase(activities):
    if not activities:
        logging.debug("Geen activiteiten om te uploaden.")
        return

    # ✅ Haal bestaande ID's op uit Supabase
    logging.debug("Ophalen bestaande activity-ID’s uit Supabase ...")
    resp = supabase.table(SUPABASE_TABLE).select("id").execute()
    existing_ids = {row["id"] for row in resp.data or []}
    logging.debug(f"{len(existing_ids)} bestaande ID’s gevonden.")

    # ✅ Alleen nieuwe activiteiten toevoegen
    new_activities = [a for a in activities if a["id"] not in existing_ids]
    logging.debug(f"{len(new_activities)} nieuwe activiteiten gevonden die ontbreken in Supabase.")

    if not new_activities:
        logging.debug("Geen nieuwe activiteiten om toe te voegen.")
        return

    try:
        resp = supabase.table(SUPABASE_TABLE).insert(new_activities).execute()
        if getattr(resp, "data", None):
            logging.debug(f"{len(resp.data)} activiteiten toegevoegd aan Supabase.")
        else:
            logging.warning("Geen data terug ontvangen van Supabase na insert.")
    except Exception as e:
        logging.error(f"Fout bij upload naar Supabase: {e}")


# ======================
# MAIN SCRIPT
# ======================
def main():
    logging.basicConfig(level=logging.DEBUG, format="DEBUG: %(message)s")
    logging.debug("Start sync - laatste 30 dagen")

    token = get_access_token()
    activities = fetch_recent_activities(token, days=30)
    save_to_files(activities)

    prepared = prepare_activities(activities)
    upload_to_supabase(prepared)

    logging.debug("Sync afgerond ✅")


if __name__ == "__main__":
    main()
