import os
import csv
import json
import time
from datetime import datetime, timedelta

import requests
import polyline
import psycopg2


def env(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_access_token() -> str:
    # Allow overriding with a direct access token (useful for quick testing)
    direct = os.environ.get("STRAVA_ACCESS_TOKEN")
    if direct:
        return direct

    client_id = env("STRAVA_CLIENT_ID")
    client_secret = env("STRAVA_CLIENT_SECRET")

    # Try to load refresh token from DB store if configured; fallback to env
    store = TokenStore.from_env()
    refresh_token = store.load_refresh_token() if store else env("STRAVA_REFRESH_TOKEN")

    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    response = requests.post(url, data=payload, timeout=30)
    if response.status_code == 401:
        raise RuntimeError(
            "Unauthorized when exchanging refresh token. Check STRAVA_CLIENT_ID/SECRET/REFRESH_TOKEN and ensure the token was obtained with scope 'read,activity:read_all'."
        )
    response.raise_for_status()
    data = response.json()

    # Persist rotated refresh token if present
    new_refresh = data.get("refresh_token")
    expires_in = data.get("expires_in")
    if new_refresh and store:
        expires_at = int(datetime.now().timestamp()) + int(expires_in or 0)
        store.save_tokens(
            access_token=data.get("access_token"),
            refresh_token=new_refresh,
            expires_at=expires_at,
        )
    return data["access_token"]


class TokenStore:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._ensure_table()

    @staticmethod
    def from_env():
        host = os.environ.get("DB_HOST")
        user = os.environ.get("DB_USER")
        password = os.environ.get("DB_PASSWORD")
        name = os.environ.get("DB_NAME", "postgres")
        port = os.environ.get("DB_PORT", "5432")
        if not (host and user and password):
            return None
        dsn = (
            f"host={host} user={user} password={password} dbname={name} port={port}"
        )
        return TokenStore(dsn)

    def _ensure_table(self) -> None:
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS oauth_tokens (
                        provider TEXT PRIMARY KEY,
                        access_token TEXT,
                        refresh_token TEXT NOT NULL,
                        expires_at BIGINT
                    )
                    """
                )
                conn.commit()

    def load_refresh_token(self) -> str:
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT refresh_token FROM oauth_tokens WHERE provider = %s",
                    ("strava",),
                )
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
        # If not found, fallback to env in caller
        return env("STRAVA_REFRESH_TOKEN")

    def save_tokens(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        expires_at: int | None,
    ) -> None:
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO oauth_tokens (provider, access_token, refresh_token, expires_at)
                    VALUES ($$strava$$, %s, %s, %s)
                    ON CONFLICT (provider) DO UPDATE SET
                        access_token = EXCLUDED.access_token,
                        refresh_token = EXCLUDED.refresh_token,
                        expires_at = EXCLUDED.expires_at
                    """,
                    (access_token, refresh_token, expires_at),
                )
                conn.commit()


def debug_log(message: str, log_path: str) -> None:
    now = datetime.now().isoformat()
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{now} - {message}\n")
    print(f"DEBUG: {message}")


def read_existing_ids(csv_file: str) -> set[str]:
    ids: set[str] = set()
    if not os.path.exists(csv_file):
        return ids
    try:
        with open(csv_file, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if "ID" in row and row["ID"]:
                    ids.add(str(row["ID"]))
    except Exception:
        pass
    return ids


def get_last_activity_date(csv_file: str, log_path: str) -> datetime | None:
    if not os.path.exists(csv_file):
        debug_log("Geen bestaande CSV gevonden; volledige sync.", log_path)
        return None
    dates: list[datetime] = []
    try:
        with open(csv_file, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                value = row.get("Datum")
                if not value or value == "Onbekend":
                    continue
                for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        dates.append(datetime.strptime(value, fmt))
                        break
                    except ValueError:
                        continue
    except Exception as e:
        debug_log(f"Fout bij lezen laatste datum: {e}", log_path)
    if not dates:
        return None
    return max(dates)


def fetch_activities(access_token: str, after: datetime | None, log_path: str) -> list[dict]:
    activities: list[dict] = []
    page = 1
    per_page = 200
    headers = {"Authorization": f"Bearer {access_token}"}
    while True:
        params: dict[str, int] = {"page": page, "per_page": per_page}
        if after is not None:
            params["after"] = int(after.timestamp())
        resp = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers=headers,
            params=params,
            timeout=60,
        )
        if resp.status_code == 401:
            # Provide actionable hint
            raise RuntimeError(
                "401 Unauthorized fetching activities. Likely invalid/expired access credentials. Verify STRAVA_REFRESH_TOKEN (and scopes) or use STRAVA_ACCESS_TOKEN to test."
            )
        if resp.status_code == 429:
            debug_log("Rate limit bereikt; 60s pauze.", log_path)
            time.sleep(60)
            continue
        resp.raise_for_status()
        page_activities = resp.json()
        if not page_activities:
            break
        activities.extend(page_activities)
        debug_log(f"Pagina {page} -> {len(page_activities)} activiteiten.", log_path)
        page += 1
    debug_log(f"Totaal opgehaald: {len(activities)}", log_path)
    return activities


def fetch_activity_details(activity_id: int, access_token: str, log_path: str) -> dict | None:
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"include_all_efforts": True, "keys_by_type": True}
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    for attempt in range(3):
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        if resp.status_code == 429:
            debug_log("Details rate limit; 60s pauze.", log_path)
            time.sleep(60)
            continue
        if resp.ok:
            return resp.json()
        time.sleep(2 ** attempt)
    debug_log(f"Details ophalen mislukt voor {activity_id}", log_path)
    return None


def save_json(all_activities: list[dict], json_file: str, log_path: str) -> None:
    if os.path.exists(json_file):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []
        existing.extend(all_activities)
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(existing, f)
        debug_log(f"JSON bijgewerkt: {json_file}", log_path)
    else:
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(all_activities, f)
        debug_log(f"JSON geschreven: {json_file}", log_path)


def prepare_csv_row(activity: dict) -> dict:
    row: dict = {
        "ID": activity.get("id", "Onbekend"),
        "Naam": activity.get("name", "Onbekend"),
        "Datum": activity.get("start_date", "Onbekend"),
        "Type": activity.get("type", "Onbekend"),
        "Afstand (km)": round(activity.get("distance", 0) / 1000, 2),
        "Tijd (min)": round(activity.get("moving_time", 0) / 60, 2),
        "Totale tijd (min)": round(activity.get("elapsed_time", 0) / 60, 2),
        "Hoogtemeters": activity.get("total_elevation_gain", 0),
        "Gemiddelde snelheid (km/u)": round(activity.get("average_speed", 0) * 3.6, 2),
        "Max snelheid (km/u)": round(activity.get("max_speed", 0) * 3.6, 2),
        "Gemiddelde hartslag": activity.get("average_heartrate"),
        "Max hartslag": activity.get("max_heartrate"),
    }
    return row


def save_csv(activities: list[dict], csv_file: str, log_path: str) -> int:
    file_exists = os.path.exists(csv_file)
    existing_ids = read_existing_ids(csv_file)
    new_rows = []
    for activity in activities:
        activity_id = str(activity.get("id", ""))
        if not activity_id or activity_id in existing_ids:
            continue
        new_rows.append(prepare_csv_row(activity))

    if not new_rows:
        debug_log("Geen nieuwe rijen voor CSV.", log_path)
        return 0

    fieldnames = list(new_rows[0].keys())
    mode = "a" if file_exists else "w"
    with open(csv_file, mode=mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)
    debug_log(f"CSV opgeslagen: +{len(new_rows)} rijen -> {csv_file}", log_path)
    return len(new_rows)


def save_gps_csv(activities: list[dict], gps_file: str, log_path: str) -> None:
    with open(gps_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "ActivityID",
                "ActivityName",
                "ActivityType",
                "Latitude",
                "Longitude",
                "Timestamp",
                "Distance",
                "Elevation",
            ],
        )
        writer.writeheader()
        for activity in activities:
            m = activity.get("map") or {}
            summary = m.get("summary_polyline")
            if not summary:
                continue
            try:
                points = polyline.decode(summary)
            except Exception:
                continue
            if not points:
                continue
            start = activity.get("start_date")
            if not start:
                continue
            try:
                start_dt = datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                continue
            total_points = len(points)
            duration = int(activity.get("moving_time", 0) or 0)
            if total_points == 0 or duration == 0:
                continue
            time_per_point = duration / total_points
            total_distance = float(activity.get("distance", 0) or 0)
            total_elevation = float(activity.get("total_elevation_gain", 0) or 0)
            dist_per = total_distance / total_points if total_points else 0
            elev_per = total_elevation / total_points if total_points else 0
            for idx, (lat, lng) in enumerate(points):
                ts = start_dt + timedelta(seconds=idx * time_per_point)
                writer.writerow(
                    {
                        "ActivityID": activity.get("id"),
                        "ActivityName": activity.get("name"),
                        "ActivityType": activity.get("type"),
                        "Latitude": lat,
                        "Longitude": lng,
                        "Timestamp": ts.isoformat(),
                        "Distance": dist_per,
                        "Elevation": elev_per,
                    }
                )
    debug_log(f"GPS CSV geschreven: {gps_file}", log_path)


def main() -> None:
    # Bestanden (repo-relatief)
    csv_file = os.environ.get("CSV_FILE", "activiteiten.csv")
    json_file = os.environ.get("JSON_FILE", "activiteiten_raw.json")
    gps_file = os.environ.get("GPS_FILE", "strava_gps_data.csv")
    log_file = os.environ.get("DEBUG_LOG", "debug_log.txt")

    os.makedirs(os.path.dirname(csv_file) or ".", exist_ok=True)

    debug_log("Start Strava sync (CI)", log_file)
    token = get_access_token()
    last_date = get_last_activity_date(csv_file, log_file)
    activities_summary = fetch_activities(token, last_date, log_file)

    detailed_activities: list[dict] = []
    for act in activities_summary:
        act_id = act.get("id")
        if not act_id:
            continue
        det = fetch_activity_details(int(act_id), token, log_file)
        detailed_activities.append(det if det else act)

    if not detailed_activities:
        debug_log("Geen nieuwe activiteiten gevonden.", log_file)
        return

    save_json(detailed_activities, json_file, log_file)
    save_csv(detailed_activities, csv_file, log_file)
    save_gps_csv(detailed_activities, gps_file, log_file)
    debug_log("Sync gereed.", log_file)


if __name__ == "__main__":
    main()


