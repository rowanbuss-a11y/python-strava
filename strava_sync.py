import os
import json
import time
from datetime import datetime, timedelta
import requests

def env(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def get_access_token() -> str:
    """
    Haal access token op via refresh token of STRAVA_AUTH_CODE
    """
    client_id = env("STRAVA_CLIENT_ID")
    client_secret = env("STRAVA_CLIENT_SECRET")
    refresh_token = env("STRAVA_REFRESH_TOKEN")
    redirect_uri = os.environ.get("STRAVA_REDIRECT_URI")

    # Probeer refresh token
    if refresh_token:
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        r = requests.post("https://www.strava.com/oauth/token", data=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        print("DEBUG: Nieuw access token verkregen")
        return data["access_token"]

    # Fallback naar auth code
    auth_code = os.environ.get("STRAVA_AUTH_CODE")
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
        print("DEBUG: Access token opgehaald via auth code")
        return data["access_token"]

    raise RuntimeError("Geen geldig access token of auth code gevonden")

def fetch_recent_activities(access_token: str, days: int = 30) -> list[dict]:
    """
    Haal activiteiten van de laatste N dagen op
    """
    activities = []
    page = 1
    per_page = 200
    after_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())

    print(f"DEBUG: Fetching activities from last {days} days")
    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"after": after_timestamp, "page": page, "per_page": per_page}

        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code == 429:
            print("DEBUG: Rate limit, wacht 60s...")
            time.sleep(60)
            continue
        r.raise_for_status()

        page_activities = r.json()
        if not page_activities:
            break

        print(f"DEBUG: Pagina {page} → {len(page_activities)} activiteiten")
        activities.extend(page_activities)
        page += 1

    print(f"DEBUG: Totaal {len(activities)} activiteiten opgehaald")
    return activities

def upload_to_supabase(activities: list[dict]) -> None:
    """
    Upload activiteiten naar Supabase via REST API
    """
    if not activities:
        print("DEBUG: Geen activiteiten om te uploaden")
        return

    supabase_url = env("SUPABASE_URL")
    supabase_key = env("SUPABASE_KEY")

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    prepared = []
    for act in activities:
        prepared.append({
            "id": int(act.get("id")),
            "name": act.get("name") or "",
            "type": act.get("type") or "",
            "start_date": act.get("start_date"),
            "distance": float(act.get("distance", 0)) / 1000,
            "moving_time": int(act.get("moving_time", 0)),
            "elapsed_time": int(act.get("elapsed_time", 0)),
            "total_elevation_gain": float(act.get("total_elevation_gain", 0)),
            "average_speed": float(act.get("average_speed", 0) * 3.6),
            "max_speed": float(act.get("max_speed", 0) * 3.6),
            "average_heartrate": float(act.get("average_heartrate") or 0),
            "max_heartrate": float(act.get("max_heartrate") or 0),
            "gear_id": act.get("gear_id"),
            "trainer": bool(act.get("trainer", False)),
            "commute": bool(act.get("commute", False)),
            "private": bool(act.get("private", False)),
            "description": act.get("description") or ""
        })

    try:
        url = f"{supabase_url}/rest/v1/strava_activities"
        r = requests.post(url, headers=headers, json=prepared, timeout=30)
        if r.status_code in (200, 201):
            print(f"DEBUG: Uploaded {len(prepared)} activities to Supabase ✅")
        else:
            print(f"DEBUG: Upload error: {r.status_code} {r.text}")
    except Exception as e:
        print(f"DEBUG: Exception tijdens upload: {e}")

def main():
    days = int(os.environ.get("DAYS_BACK", "30"))

    token = get_access_token()
    activities = fetch_recent_activities(token, days=days)

    if activities:
        upload_to_supabase(activities)

if __name__ == "__main__":
    main()
