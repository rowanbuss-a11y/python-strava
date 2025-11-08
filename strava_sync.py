import os
import requests
import datetime
import json

# ==============
# CONFIG
# ==============

STRAVA_API_URL = "https://www.strava.com/api/v3"


# ==========================
# TOKEN MANAGEMENT
# ==========================

def refresh_access_token():
    """Vernieuw het Strava access token met de refresh token"""
    client_id = os.environ.get("STRAVA_CLIENT_ID")
    client_secret = os.environ.get("STRAVA_CLIENT_SECRET")
    refresh_token = os.environ.get("STRAVA_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("Strava clientgegevens ontbreken in environment variables")

    response = requests.post(
        "https://www.strava.com/api/v3/oauth/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
    )

    if response.status_code != 200:
        raise RuntimeError(f"Kon access token niet vernieuwen: {response.text}")

    token_data = response.json()
    print("DEBUG: New refresh token - update STRAVA_REFRESH_TOKEN Secret")
    return token_data["access_token"]


# ==========================
# FETCH ACTIVITIES
# ==========================

def fetch_recent_activities(access_token, days=30):
    """Haal activiteiten van de afgelopen X dagen op"""
    after_date = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    after_timestamp = int(after_date.timestamp())

    headers = {"Authorization": f"Bearer {access_token}"}
    page = 1
    all_activities = []

    print(f"DEBUG: Fetching activities from last {days} days (after {after_date.date()})")

    while True:
        params = {"after": after_timestamp, "page": page, "per_page": 100}
        r = requests.get(f"{STRAVA_API_URL}/athlete/activities", headers=headers, params=params)

        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized - token may be invalid")

        activities = r.json()
        if not activities:
            break

        print(f"DEBUG: Page {page}: {len(activities)} activities")
        all_activities.extend(activities)
        page += 1

    print(f"DEBUG: Total: {len(all_activities)} activities")
    return all_activities


# ==========================
# SAVE TO SUPABASE
# ==========================

def save_to_supabase(activities):
    """Sla Strava-data op in Supabase via de REST API"""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    table = os.environ.get("SUPABASE_TABLE", "strava_activities")

    if not url or not key:
        print("DEBUG: Supabase API niet geconfigureerd, skip upload")
        return

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    endpoint = f"{url}/rest/v1/{table}"

    rows = []
    for act in activities:
        start_latlng = act.get("start_latlng", [])
        end_latlng = act.get("end_latlng", [])
        rows.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date"),
            "distance": act.get("distance"),
            "moving_time": act.get("moving_time"),
            "elapsed_time": act.get("elapsed_time"),
            "total_elevation_gain": act.get("total_elevation_gain"),
            "average_speed": act.get("average_speed"),
            "max_speed": act.get("max_speed"),
            "average_heartrate": act.get("average_heartrate"),
            "max_heartrate": act.get("max_heartrate"),
            "start_latitude": start_latlng[0] if start_latlng else None,
            "start_longitude": start_latlng[1] if len(start_latlng) > 1 else None,
            "end_latitude": end_latlng[0] if end_latlng else None,
            "end_longitude": end_latlng[1] if len(end_latlng) > 1 else None,
            "timezone": act.get("timezone"),
            "utc_offset": act.get("utc_offset"),
            "kudos_count": act.get("kudos_count", 0),
            "comment_count": act.get("comment_count", 0),
            "gear_id": act.get("gear_id"),
            "trainer": act.get("trainer", False),
            "commute": act.get("commute", False),
            "private": act.get("private", False),
            "description": act.get("description"),
        })

    r = requests.post(endpoint, headers=headers, json=rows, timeout=60)
    if r.status_code not in (200, 201, 204):
        print(f"DEBUG: Supabase insert failed ({r.status_code}): {r.text[:500]}")
    else:
        print(f"DEBUG: Saved {len(rows)} activities to Supabase")


# ==========================
# SAVE LOCAL FILES
# ==========================

def save_local(activities):
    with open("activiteiten_raw.json", "w") as f:
        json.dump(activities, f, indent=2)
    print(f"DEBUG: Saved {len(activities)} activities to activiteiten_raw.json")

    import csv
    with open("activiteiten.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "type", "distance", "start_date"])
        for a in activities:
            writer.writerow([a.get("id"), a.get("name"), a.get("type"), a.get("distance"), a.get("start_date")])
    print(f"DEBUG: Saved {len(activities)} new activities to activiteiten.csv")


# ==========================
# MAIN
# ==========================

def main():
    print("DEBUG: Start sync - laatste 30 dagen")
    access_token = refresh_access_token()
    activities = fetch_recent_activities(access_token)
    save_local(activities)
    save_to_supabase(activities)
    print("DEBUG: Sync gereed")


if __name__ == "__main__":
    main()
