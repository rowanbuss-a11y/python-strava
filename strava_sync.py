import os
import csv
import json
import time
import requests
from datetime import datetime, timedelta


def get_env(name, required=True, default=None):
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def get_access_token():
    """Gebruik refresh token om nieuw access token op te halen"""
    client_id = get_env("STRAVA_CLIENT_ID")
    client_secret = get_env("STRAVA_CLIENT_SECRET")
    refresh_token = get_env("STRAVA_REFRESH_TOKEN")

    response = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    print("DEBUG: Nieuw access token verkregen")
    return data["access_token"]


def fetch_recent_activities(access_token, days=30):
    """Haal Strava activiteiten op van de laatste X dagen"""
    activities = []
    after = int((datetime.now() - timedelta(days=days)).timestamp())
    page = 1
    while True:
        r = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"per_page": 200, "page": page, "after": after},
            timeout=60,
        )
        if r.status_code == 429:
            time.sleep(60)
            continue
        r.raise_for_status()
        page_data = r.json()
        if not page_data:
            break
        activities.extend(page_data)
        print(f"DEBUG: Pagina {page} → {len(page_data)} activiteiten")
        page += 1
    print(f"DEBUG: Totaal {len(activities)} activiteiten opgehaald")
    return activities


def save_to_supabase(activities):
    """Sla activiteiten rechtstreeks op in Supabase via REST API"""
    if not activities:
        print("DEBUG: Geen activiteiten om op te slaan")
        return

    url = f"{get_env('SUPABASE_URL')}/rest/v1/strava_activities"
    headers = {
        "apikey": get_env("SUPABASE_SERVICE_ROLE_KEY"),
        "Authorization": f"Bearer {get_env('SUPABASE_SERVICE_ROLE_KEY')}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    payload = []
    for act in activities:
        payload.append({
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
            "timezone": act.get("timezone"),
            "utc_offset": act.get("utc_offset"),
            "kudos_count": act.get("kudos_count"),
            "comment_count": act.get("comment_count"),
            "commute": act.get("commute"),
            "trainer": act.get("trainer"),
            "private": act.get("private"),
            "description": act.get("description"),
        })

    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code not in (200, 201, 204):
        print(f"DEBUG: Fout bij upload: {r.status_code} {r.text[:300]}")
    else:
        print(f"DEBUG: {len(activities)} activiteiten naar Supabase geüpload")


def save_to_csv(activities, filename):
    """Sla activiteiten ook lokaal op als CSV"""
    if not activities:
        return
    fieldnames = ["id", "name", "type", "start_date", "distance", "moving_time"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for a in activities:
            writer.writerow({k: a.get(k) for k in fieldnames})
    print(f"DEBUG: {len(activities)} activiteiten opgeslagen in {filename}")


def save_to_json(activities, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(activities, f, indent=2)
    print(f"DEBUG: {len(activities)} activiteiten opgeslagen in {filename}")


def main():
    token = get_access_token()
    activities = fetch_recent_activities(token, days=30)
    save_to_supabase(activities)
    save_to_csv(activities, "activiteiten.csv")
    save_to_json(activities, "activiteiten_raw.json")
    print("DEBUG: Sync afgerond ✅")


if __name__ == "__main__":
    main()
