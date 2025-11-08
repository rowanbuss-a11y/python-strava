"""
Strava sync script - laatste 30 dagen
Directe upload naar Supabase/Postgres
"""

import os
import csv
import json
import time
from datetime import datetime, timedelta

import requests
import psycopg2
from psycopg2.extras import execute_values


def env(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_access_token() -> str:
    """Haal access token op via refresh token of auth code"""
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
        r.raise_for_status()
        data = r.json()
        # Update refresh token file / env
        with open("new_refresh_token.txt", "w") as f:
            f.write(data.get("refresh_token", ""))
        return data["access_token"]

    if auth_code:
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": auth_code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        }
        r = requests.post("https://www.strava.com/oauth/token", data=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        with open("new_refresh_token.txt", "w") as f:
            f.write(data.get("refresh_token", ""))
        return data["access_token"]

    raise RuntimeError("No valid STRAVA_REFRESH_TOKEN or STRAVA_AUTH_CODE set")


def fetch_recent_activities(access_token: str, days: int = 30) -> list[dict]:
    """Haal activiteiten van de laatste N dagen"""
    activities = []
    page = 1
    per_page = 200
    after_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())

    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"page": page, "per_page": per_page, "after": after_timestamp}

        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(60)
            continue
        r.raise_for_status()
        page_activities = r.json()
        if not page_activities:
            break
        activities.extend(page_activities)
        if len(page_activities) < per_page:
            break
        page += 1

    return activities


def save_to_csv(activities: list[dict], filename: str):
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
                existing_ids.add(row.get("ID"))

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
            "Afstand (km)": float(act.get("distance", 0)) / 1000,
            "Tijd (min)": float(act.get("moving_time", 0)) / 60,
            "Totale tijd (min)": float(act.get("elapsed_time", 0)) / 60,
            "Hoogtemeters": float(act.get("total_elevation_gain", 0)),
            "Gemiddelde snelheid (km/u)": float(act.get("average_speed", 0)) * 3.6,
            "Max snelheid (km/u)": float(act.get("max_speed", 0)) * 3.6,
            "Gemiddelde hartslag": float(act.get("average_heartrate") or 0),
            "Max hartslag": float(act.get("max_heartrate") or 0),
        })

    if not new_rows:
        return

    with open(filename, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)


def save_to_json(activities: list[dict], filename: str):
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


def init_db_config() -> dict | None:
    host = os.environ.get("DB_HOST")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    name = os.environ.get("DB_NAME", "postgres")
    port = os.environ.get("DB_PORT", "5432")

    if not (host and user and password):
        return None

    return {"host": host, "user": user, "password": password, "dbname": name, "port": port}


def save_to_db(activities: list[dict], db_config: dict):
    """Opslaan in Supabase/Postgres"""
    if not activities or not db_config:
        return

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                rows = []
                for act in activities:
                    start_latlng = act.get("start_latlng") or [None, None]
                    end_latlng = act.get("end_latlng") or [None, None]
                    rows.append((
                        int(act.get("id")),
                        act.get("name"),
                        act.get("type"),
                        act.get("start_date"),
                        float(act.get("distance", 0)),
                        int(act.get("moving_time", 0)),
                        int(act.get("elapsed_time", 0)),
                        float(act.get("total_elevation_gain", 0)),
                        float(act.get("average_speed", 0)),
                        float(act.get("max_speed", 0)),
                        float(act.get("average_heartrate") or 0),
                        float(act.get("max_heartrate") or 0),
                        float(start_latlng[0] or 0),
                        float(start_latlng[1] or 0),
                        float(end_latlng[0] or 0),
                        float(end_latlng[1] or 0),
                        act.get("timezone"),
                        act.get("utc_offset"),
                        int(act.get("kudos_count", 0)),
                        int(act.get("comment_count", 0)),
                        act.get("gear_id"),
                        act.get("trainer", False),
                        act.get("commute", False),
                        act.get("private", False),
                        act.get("description"),
                    ))

                execute_values(cur, """
                    INSERT INTO strava_activities
                    (id, name, type, start_date, distance, moving_time, elapsed_time,
                    total_elevation_gain, average_speed, max_speed, average_heartrate,
                    max_heartrate, start_latitude, start_longitude, end_latitude, end_longitude,
                    timezone, utc_offset, kudos_count, comment_count, gear_id, trainer, commute, private, description)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name, type=EXCLUDED.type, start_date=EXCLUDED.start_date,
                    distance=EXCLUDED.distance, moving_time=EXCLUDED.moving_time,
                    elapsed_time=EXCLUDED.elapsed_time, total_elevation_gain=EXCLUDED.total_elevation_gain,
                    average_speed=EXCLUDED.average_speed, max_speed=EXCLUDED.max_speed,
                    average_heartrate=EXCLUDED.average_heartrate, max_heartrate=EXCLUDED.max_heartrate,
                    start_latitude=EXCLUDED.start_latitude, start_longitude=EXCLUDED.start_longitude,
                    end_latitude=EXCLUDED.end_latitude, end_longitude=EXCLUDED.end_longitude,
                    timezone=EXCLUDED.timezone, utc_offset=EXCLUDED.utc_offset,
                    kudos_count=EXCLUDED.kudos_count, comment_count=EXCLUDED.comment_count,
                    gear_id=EXCLUDED.gear_id, trainer=EXCLUDED.trainer,
                    commute=EXCLUDED.commute, private=EXCLUDED.private,
                    description=EXCLUDED.description
                """, rows)
                conn.commit()
    except Exception as e:
        print(f"DEBUG: Fout bij upload: {e}")


def main():
    csv_file = os.environ.get("CSV_FILE", "activiteiten.csv")
    json_file = os.environ.get("JSON_FILE", "activiteiten_raw.json")
    days = int(os.environ.get("DAYS_BACK", "30"))

    print(f"DEBUG: Start sync - laatste {days} dagen")

    db_config = init_db_config()
    access_token = get_access_token()
    activities = fetch_recent_activities(access_token, days)

    print(f"DEBUG: Pagina 1 → {len(activities)} activiteiten")
    print(f"DEBUG: Totaal {len(activities)} activiteiten opgehaald")

    save_to_csv(activities, csv_file)
    save_to_json(activities, json_file)
    save_to_db(activities, db_config)

    print("DEBUG: Sync afgerond ✅")


if __name__ == "__main__":
    main()
