import os
import requests
import json
from supabase import create_client, Client

# --------------------------------------------------
# Configuratie
# --------------------------------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = os.environ.get("STRAVA_REFRESH_TOKEN")

SUPABASE_TABLE = "strava_activities"
SUPABASE: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --------------------------------------------------
# Strava OAuth refresh
# --------------------------------------------------
def refresh_strava_token():
    print("DEBUG: Refreshing Strava access token...")
    response = requests.post(
        "https://www.strava.com/api/v3/oauth/token",
        data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": STRAVA_REFRESH_TOKEN,
        },
    )
    response.raise_for_status()
    token_data = response.json()
    print("DEBUG: Nieuw access token verkregen")
    return token_data["access_token"]

# --------------------------------------------------
# Activiteiten ophalen
# --------------------------------------------------
def fetch_recent_activities(access_token, days=60):
    print(f"DEBUG: Fetching activities from last {days} days")
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"per_page": 200, "page": 1}
    all_acts = []

    while True:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        page_data = r.json()
        if not page_data:
            break
        all_acts.extend(page_data)
        if len(page_data) < 200:
            break
        params["page"] += 1

    print(f"DEBUG: Total {len(all_acts)} activities fetched")
    return all_acts

# --------------------------------------------------
# Upload naar Supabase (alleen calorieën)
# --------------------------------------------------
def upload_to_supabase(activities):
    print("DEBUG: Uploading calories to Supabase...")

    payload = []
    skipped = 0

    for act in activities:
        calories = act.get("calories")
        if calories and calories > 0:
            payload.append({
                "id": act.get("id"),
                "name": act.get("name"),
                "type": act.get("type"),
                "start_date": act.get("start_date_local"),
                "calories": calories
            })
        else:
            skipped += 1

    print(f"DEBUG: {skipped} activiteiten overgeslagen zonder calorieën")

    if not payload:
        print("⚠️ Geen activiteiten met calorieën gevonden — upload wordt overgeslagen.")
        return

    try:
        SUPABASE.table(SUPABASE_TABLE).upsert(payload, on_conflict="id").execute()
        print(f"✅ Uploaded {len(payload)} records to Supabase")
    except Exception as e:
        print(f"❌ ERROR: Upload to Supabase failed → {e}")

# --------------------------------------------------
# Full refresh (verwijder oude activiteiten in periode en upload opnieuw)
# --------------------------------------------------
def full_refresh_supabase(activities):
    if not activities:
        print("⚠️ Geen activiteiten gevonden voor full refresh.")
        return

    min_date = min(act["start_date_local"] for act in activities)
    max_date = max(act["start_date_local"] for act in activities)

    try:
        SUPABASE.table(SUPABASE_TABLE).delete().gte("start_date", min_date).lte("start_date", max_date).execute()
        print(f"DEBUG: Bestaande activiteiten van {min_date} t/m {max_date} verwijderd.")
    except Exception as e:
        print(f"❌ ERROR bij verwijderen bestaande records → {e}")

    upload_to_supabase(activities)

# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    token = refresh_strava_token()
    activities = fetch_recent_activities(token, days=60)

    # Opslaan van raw JSON voor debugging
    with open("activiteiten_raw.json", "w") as f:
        json.dump(activities, f, indent=2)

    # Full refresh naar Supabase
    full_refresh_supabase(activities)
    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
