"""
Strava sync script - laatste 30 dagen
Haal recente activiteiten op en sla op in CSV, JSON en Supabase
"""

import os
import csv
import json
import time
from datetime import datetime, timedelta
import requests

# --- Helper functies ---

def env(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

# --- Strava OAuth ---

def get_access_token() -> str:
    client_id = env("STRAVA_CLIENT_ID")
    client_secret = env("STRAVA_CLIENT_SECRET")
    refresh_token = os.environ.get("STRAVA_REFRESH_TOKEN")
    auth_code = os.environ.get("STRAVA_AUTH_CODE")
    redirect_uri = os.environ.get("STRAVA_REDIRECT_URI")

    if refresh_token:
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        r = requests.post("https://www.strava.com/oauth/token", data=payload, timeout=30)
        if r.status_code == 200:
            data = r.json()
            new_refresh = data.get("refresh_token")
            if new_refresh:
                print("DEBUG: Nieuw refresh token - update STRAVA_REFRESH_TOKEN Secret")
                with open("new_refresh_token.txt", "w") as f:
                    f.write(new_refresh)
            return data["access_token"]

    if auth_code:
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": auth_code,
            "grant_type": "authorization_code",
        }
        if redirect_uri:
            payload["redirect_uri"] = redirect_uri
        r = requests.post("https://www.strava.com/oauth/token", data=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data["access_token"]

    raise RuntimeError("No valid refresh token or auth code. Set STRAVA_REFRESH_TOKEN or STRAVA_AUTH_CODE")

# --- Fetch activiteiten ---

def fetch_recent_activities(access_token: str, days: int = 30) -> list[dict]:
    activities = []
    page = 1
    per_page = 200
    after_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())

    print(f"DEBUG: Fetching activities from last {days} days")

    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"page": page, "per_page": per_page, "after": after_timestamp}

        r = requests.get(url, headers=headers, params=params, timeout=60)

        if r.status_code == 429:
            print("DEBUG: Rate limit - waiting 60s...")
            time.sleep(60)
            continue
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized - token may be invalid")
        r.raise_for_status()

        page_activities = r.json()
        if not page_activities:
            break
        activities.extend(page_activities)
        print(f"DEBUG: Page {page} → {len(page_activities)} activities")

        if len(page_activities) < per_page:
            break
        page += 1

    print(f"DEBUG: Total {len(activities)} activities fetched")
    return activities

# --- CSV / JSON opslaan ---

def save_to_csv(activities: list[dict], filename: str) -> None:
    if not activities:
        return

    fieldnames = [
        "ID", "Naam", "Datum", "Type", "Afstand (km)", "Tijd (min)",
        "Totale tijd (min)", "Hoogtemeters", "Gemiddelde snelheid (km/u)",
        "Max snelheid (km/u)", "Gemiddelde hartslag", "Max hartslag",
    ]

    file_exists = os.path.exists(filename)
    existing_ids = set()

    if file_exists:
        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "ID" in row:
                    existing_ids.add(row["ID"])

    new_rows = []
    for act in activities:
        act_id = str(act.get("id", ""))
        if act_id in existing_ids:
            continue
        new_rows.append({
            "ID": act_id,
            "Naam": act.get("name", ""),
            "Datum": act.get("start_date", ""),
            "Type": act.get("type", ""),
            "Afstand (km)": round(act.get("distance", 0) / 1000, 2),
            "Tijd (min)": round(act.get("moving_time", 0) / 60, 2),
            "Totale tijd (min)": round(act.get("elapsed_time", 0) / 60, 2),
            "Hoogtemeters": act.get("total_elevation_gain", 0),
            "Gemiddelde snelheid (km/u)": round(act.get("average_speed", 0) * 3.6, 2),
            "Max snelheid (km/u)": round(act.get("max_speed", 0) * 3.6, 2),
            "Gemiddelde hartslag": act.get("average_heartrate"),
            "Max hartslag": act.get("max_heartrate"),
        })

    if not new_rows:
        print("DEBUG: No new activities to add")
        return

    mode = "a" if file_exists else "w"
    with open(filename, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)

    print(f"DEBUG: {len(new_rows)} activities saved to {filename}")

def save_to_json(activities: list[dict], filename: str) -> None:
    existing = []
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []
    existing.extend(activities)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(existing, f)
    print(f"DEBUG: {len(activities)} activities saved to {filename}")

# --- Supabase upload ---

def upload_to_supabase(activities: list[dict]) -> None:
    import requests

    supabase_url = env("SUPABASE_URL")
    supabase_key = env("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        print("DEBUG: SUPABASE_URL or SUPABASE_KEY missing")
        return

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    prepared = []
    for act in activities:
        try:
            prepared.append({
                "id": int(act.get("id")),
                "name": act.get("name") or "",
                "type": act.get("type") or "",
                "start_date": act.get("start_date"),
                "distance": round(float(act.get("distance", 0)) / 1000, 2),
                "moving_time": int(round(act.get("moving_time", 0))),
                "elapsed_time": int(round(act.get("elapsed_time", 0))),
                "total_elevation_gain": float(act.get("total_elevation_gain", 0)),
                "average_speed": round(float(act.get("average_speed", 0)) * 3.6, 2),
                "max_speed": round(float(act.get("max_speed", 0)) * 3.6, 2),
                "average_heartrate": float(act.get("average_heartrate")) if act.get("average_heartrate") else None,
                "max_heartrate": float(act.get("max_heartrate")) if act.get("max_heartrate") else None,
                "start_latitude": float(act.get("start_latlng")[0]) if act.get("start_latlng") else None,
                "start_longitude": float(act.get("start_latlng")[1]) if act.get("start_latlng") else None,
                "end_latitude": float(act.get("end_latlng")[0]) if act.get("end_latlng") else None,
                "end_longitude": float(act.get("end_latlng")[1]) if act.get("end_latlng") else None,
                "timezone": act.get("timezone") or "",
                "utc_offset": int(act.get("utc_offset", 0)),
                "kudos_count": int(act.get("kudos_count", 0)),
                "comment_count": int(act.get("comment_count", 0)),
                "gear_id": act.get("gear_id"),
                "trainer": bool(act.get("trainer", False)),
                "commute": bool(act.get("commute", False)),
                "private": bool(act.get("private", False)),
                "description": act.get("description") or ""
            })
        except Exception as e:
            print(f"DEBUG: Error preparing activity {act.get('id')}: {e}")

    if not prepared:
        print("DEBUG: No activities to upload")
        return

    try:
        r = requests.post(f"{supabase_url}/rest/v1/strava_activities", headers=headers, data=json.dumps(prepared))
        if r.status_code >= 400:
            print(f"DEBUG: Upload error: {r.status_code} {r.text}")
        else:
            print(f"DEBUG: {len(prepared)} activities successfully uploaded to Supabase")
    except Exception as e:
        print(f"DEBUG: Upload exception: {e}")

# --- Main ---

def main() -> None:
    csv_file = os.environ.get("CSV_FILE", "activiteiten.csv")
    json_file = os.environ.get("JSON_FILE", "activiteiten_raw.json")
    days = int(os.environ.get("DAYS_BACK", "30"))

    print(f"DEBUG: Start sync - laatste {days} dagen")

    token = get_access_token()
    activities = fetch_recent_activities(token, days=days)

    if not activities:
        print("DEBUG: Geen nieuwe activiteiten gevonden")
        return

    save_to_csv(activities, csv_file)
    save_to_json(activities, json_file)
    upload_to_supabase(activities)
    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
