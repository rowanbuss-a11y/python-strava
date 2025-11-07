"""
Strava sync script - laatste 30 dagen alleen
Vereenvoudigde versie die alleen recente activiteiten ophaalt
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


def exchange_authorization_code(client_id: str, client_secret: str, auth_code: str) -> dict:
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": auth_code,
        "grant_type": "authorization_code",
    }
    redirect_uri = os.environ.get("STRAVA_REDIRECT_URI")
    if redirect_uri:
        payload["redirect_uri"] = redirect_uri

    response = requests.post(url, data=payload, timeout=30)
    if response.status_code == 400:
        error_text = response.text[:300]
        raise RuntimeError(
            f"Auth code invalid/expired. Generate fresh: "
            f"https://www.strava.com/oauth/authorize?client_id={client_id}&response_type=code&redirect_uri={redirect_uri or 'YOUR_REDIRECT_URI'}&approval_prompt=force&scope=read,activity:read_all"
        )
    response.raise_for_status()
    return response.json()


class TokenStore:
    """Token store voor Supabase/Postgres"""
    def __init__(self, dsn: str):
        self.dsn = dsn
        try:
            self._ensure_table()
        except Exception:
            raise

    @staticmethod
    def from_env():
        if os.environ.get("DISABLE_DB_TOKEN_STORE") in ("1", "true", "True"):
            return None
        host = os.environ.get("DB_HOST")
        user = os.environ.get("DB_USER")
        password = os.environ.get("DB_PASSWORD")
        name = os.environ.get("DB_NAME", "postgres")
        port = os.environ.get("DB_PORT", "5432")
        if not (host and user and password):
            return None
        dsn = f"host={host} user={user} password={password} dbname={name} port={port}"
        try:
            return TokenStore(dsn)
        except Exception:
            return None

    def _ensure_table(self) -> None:
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS oauth_tokens (
                        provider TEXT PRIMARY KEY,
                        access_token TEXT,
                        refresh_token TEXT NOT NULL,
                        expires_at BIGINT
                    )
                """)
                conn.commit()

    def load_refresh_token(self) -> str | None:
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT refresh_token FROM oauth_tokens WHERE provider = %s",
                    ("strava",),
                )
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
        return None

    def save_tokens(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        expires_at: int | None,
    ) -> None:
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO oauth_tokens (provider, access_token, refresh_token, expires_at)
                    VALUES ('strava', %s, %s, %s)
                    ON CONFLICT (provider) DO UPDATE SET
                        access_token = EXCLUDED.access_token,
                        refresh_token = EXCLUDED.refresh_token,
                        expires_at = EXCLUDED.expires_at
                """, (access_token, refresh_token, expires_at))
                conn.commit()


def get_access_token() -> str:
    direct = os.environ.get("STRAVA_ACCESS_TOKEN")
    if direct:
        return direct

    client_id = env("STRAVA_CLIENT_ID")
    client_secret = env("STRAVA_CLIENT_SECRET")

    # Try DB store first, then env
    store = TokenStore.from_env()
    refresh_token = None
    if store:
        try:
            refresh_token = store.load_refresh_token()
        except Exception:
            refresh_token = None
    if not refresh_token:
        refresh_token = os.environ.get("STRAVA_REFRESH_TOKEN")

    # Try refresh token first
    if refresh_token:
        url = "https://www.strava.com/oauth/token"
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        response = requests.post(url, data=payload, timeout=30)
        if response.status_code == 200:
            data = response.json()
            new_refresh = data.get("refresh_token")
            expires_in = data.get("expires_in")
            if new_refresh:
                if store:
                    expires_at = int(datetime.now().timestamp()) + int(expires_in or 0)
                    store.save_tokens(
                        access_token=data.get("access_token"),
                        refresh_token=new_refresh,
                        expires_at=expires_at,
                    )
                    print("DEBUG: New refresh token saved to database")
                else:
                    print("DEBUG: New refresh token - update STRAVA_REFRESH_TOKEN Secret")
                    with open("new_refresh_token.txt", "w") as f:
                        f.write(new_refresh)
            return data["access_token"]

    # Fallback to auth code
    auth_code = os.environ.get("STRAVA_AUTH_CODE")
    if auth_code:
        print("DEBUG: Using authorization code for bootstrap...")
        data = exchange_authorization_code(client_id, client_secret, auth_code)
        new_refresh = data.get("refresh_token")
        expires_in = data.get("expires_in")
        if new_refresh:
            if store:
                expires_at = int(datetime.now().timestamp()) + int(expires_in or 0)
                store.save_tokens(
                    access_token=data.get("access_token"),
                    refresh_token=new_refresh,
                    expires_at=expires_at,
                )
                print("DEBUG: New refresh token saved to database")
            else:
                print("DEBUG: New refresh token - update STRAVA_REFRESH_TOKEN Secret")
                with open("new_refresh_token.txt", "w") as f:
                    f.write(new_refresh)
        return data["access_token"]

    raise RuntimeError(
        "No valid refresh token or auth code. Set STRAVA_REFRESH_TOKEN or STRAVA_AUTH_CODE"
    )


def fetch_recent_activities(access_token: str, days: int = 30) -> list[dict]:
    """Haal activiteiten op van de laatste N dagen"""
    activities = []
    page = 1
    per_page = 200
    after_date = datetime.now() - timedelta(days=days)
    after_timestamp = int(after_date.timestamp())

    print(f"DEBUG: Fetching activities from last {days} days (after {after_date.date()})")

    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {
            "page": page,
            "per_page": per_page,
            "after": after_timestamp,
        }

        response = requests.get(url, headers=headers, params=params, timeout=60)

        if response.status_code == 429:
            print("DEBUG: Rate limit - waiting 60s...")
            time.sleep(60)
            continue

        if response.status_code == 401:
            raise RuntimeError("401 Unauthorized - token may be invalid")

        response.raise_for_status()
        page_activities = response.json()

        if not page_activities:
            break

        activities.extend(page_activities)
        print(f"DEBUG: Page {page}: {len(page_activities)} activities")

        # Stop if we got fewer than per_page (last page)
        if len(page_activities) < per_page:
            break

        page += 1

    print(f"DEBUG: Total: {len(activities)} activities")
    return activities


def save_to_csv(activities: list[dict], filename: str) -> None:
    """Sla activiteiten op in CSV"""
    if not activities:
        print("DEBUG: No activities to save")
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

    print(f"DEBUG: Saved {len(new_rows)} new activities to {filename}")


def save_to_json(activities: list[dict], filename: str) -> None:
    """Sla activiteiten op in JSON"""
    if not activities:
        return

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

    print(f"DEBUG: Saved {len(activities)} activities to {filename}")


def init_database() -> dict | None:
    """Initialiseer database connectie en tabellen"""
    host = os.environ.get("DB_HOST")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    name = os.environ.get("DB_NAME", "postgres")
    port = os.environ.get("DB_PORT", "5432")
    
    if not (host and user and password):
        return None
    
    try:
        db_config = {
            "host": host,
            "user": user,
            "password": password,
            "dbname": name,
            "port": port,
        }
        
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS strava_activities (
                        id BIGINT PRIMARY KEY,
                        name VARCHAR(255),
                        type VARCHAR(50),
                        start_date TIMESTAMP,
                        distance FLOAT,
                        moving_time INTEGER,
                        elapsed_time INTEGER,
                        total_elevation_gain FLOAT,
                        average_speed FLOAT,
                        max_speed FLOAT,
                        average_heartrate FLOAT,
                        max_heartrate FLOAT,
                        start_latitude FLOAT,
                        start_longitude FLOAT,
                        end_latitude FLOAT,
                        end_longitude FLOAT,
                        timezone VARCHAR(100),
                        utc_offset INTEGER,
                        kudos_count INTEGER,
                        comment_count INTEGER,
                        gear_id VARCHAR(100),
                        trainer BOOLEAN,
                        commute BOOLEAN,
                        private BOOLEAN,
                        description TEXT
                    )
                """)
                conn.commit()
        
        print("DEBUG: Database initialized")
        return db_config
    except Exception as e:
        print(f"DEBUG: Database init failed: {e}")
        return None


def save_to_database(activities: list[dict], db_config: dict) -> None:
    """Sla activiteiten op in Supabase"""
    if not activities or not db_config:
        return
    
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                activity_data = []
                for act in activities:
                    start_latlng = act.get("start_latlng", [])
                    end_latlng = act.get("end_latlng", [])
                    
                    activity_data.append((
                        act.get("id"),
                        act.get("name"),
                        act.get("type"),
                        act.get("start_date"),
                        act.get("distance"),
                        act.get("moving_time"),
                        act.get("elapsed_time"),
                        act.get("total_elevation_gain"),
                        act.get("average_speed"),
                        act.get("max_speed"),
                        act.get("average_heartrate"),
                        act.get("max_heartrate"),
                        start_latlng[0] if start_latlng and len(start_latlng) > 0 else None,
                        start_latlng[1] if start_latlng and len(start_latlng) > 1 else None,
                        end_latlng[0] if end_latlng and len(end_latlng) > 0 else None,
                        end_latlng[1] if end_latlng and len(end_latlng) > 1 else None,
                        act.get("timezone"),
                        act.get("utc_offset"),
                        act.get("kudos_count", 0),
                        act.get("comment_count", 0),
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
                     max_heartrate, start_latitude, start_longitude, end_latitude, 
                     end_longitude, timezone, utc_offset, kudos_count, comment_count, 
                     gear_id, trainer, commute, private, description)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        start_date = EXCLUDED.start_date,
                        distance = EXCLUDED.distance,
                        moving_time = EXCLUDED.moving_time,
                        elapsed_time = EXCLUDED.elapsed_time,
                        total_elevation_gain = EXCLUDED.total_elevation_gain,
                        average_speed = EXCLUDED.average_speed,
                        max_speed = EXCLUDED.max_speed,
                        average_heartrate = EXCLUDED.average_heartrate,
                        max_heartrate = EXCLUDED.max_heartrate,
                        start_latitude = EXCLUDED.start_latitude,
                        start_longitude = EXCLUDED.start_longitude,
                        end_latitude = EXCLUDED.end_latitude,
                        end_longitude = EXCLUDED.end_longitude,
                        timezone = EXCLUDED.timezone,
                        utc_offset = EXCLUDED.utc_offset,
                        kudos_count = EXCLUDED.kudos_count,
                        comment_count = EXCLUDED.comment_count,
                        gear_id = EXCLUDED.gear_id,
                        trainer = EXCLUDED.trainer,
                        commute = EXCLUDED.commute,
                        private = EXCLUDED.private,
                        description = EXCLUDED.description
                """, activity_data)
                
                conn.commit()
        
        print(f"DEBUG: Saved {len(activities)} activities to database")
    except Exception as e:
        print(f"DEBUG: Database save failed: {e}")


def main() -> None:
    csv_file = os.environ.get("CSV_FILE", "activiteiten.csv")
    json_file = os.environ.get("JSON_FILE", "activiteiten_raw.json")
    days = int(os.environ.get("DAYS_BACK", "30"))

    print(f"DEBUG: Start sync - laatste {days} dagen")

    # Initialize database if available
    db_config = init_database()

    token = get_access_token()
    activities = fetch_recent_activities(token, days=days)

    if not activities:
        print("DEBUG: Geen nieuwe activiteiten gevonden")
        return

    save_to_csv(activities, csv_file)
    save_to_json(activities, json_file)
    
    if db_config:
        save_to_database(activities, db_config)
    
    print("DEBUG: Sync gereed")


if __name__ == "__main__":
    main()

