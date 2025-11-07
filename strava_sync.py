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


def exchange_authorization_code(client_id: str, client_secret: str, auth_code: str) -> dict:
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": auth_code,
        "grant_type": "authorization_code",
    }
    # If provided, Strava requires redirect_uri to match the one used during authorization
    redirect_uri = os.environ.get("STRAVA_REDIRECT_URI")
    if redirect_uri:
        payload["redirect_uri"] = redirect_uri
    else:
        print("DEBUG: Warning: STRAVA_REDIRECT_URI not set. This may cause issues if your Strava app requires it.")

    response = requests.post(url, data=payload, timeout=30)
    if response.status_code == 401:
        raise RuntimeError(
            "Unauthorized exchanging authorization code. Verify STRAVA_CLIENT_ID/SECRET and that the code is fresh and scoped 'read,activity:read_all'."
        )
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        # Parse error for better messaging
        error_text = response.text[:500]
        error_msg = f"Auth code exchange failed ({response.status_code}): {error_text}"
        
        if response.status_code == 400:
            if "invalid" in error_text.lower() or '"code":"invalid"' in error_text:
                error_msg = "❌ Authorization code is invalid, expired, or already used.\n\n"
                error_msg += "Common causes:\n"
                error_msg += "1. Code is expired (Strava codes expire quickly - use within minutes)\n"
                error_msg += "2. Code was already used (one-time use only)\n"
                error_msg += "3. STRAVA_REDIRECT_URI doesn't match the one used when generating the code\n"
                error_msg += "4. Code was copied incorrectly (check for extra characters)\n\n"
                error_msg += "To fix:\n"
                error_msg += "1. Generate a FRESH authorization code:\n"
                redirect_uri_hint = redirect_uri if redirect_uri else "YOUR_REDIRECT_URI"
                error_msg += f"   https://www.strava.com/oauth/authorize?client_id={client_id}&response_type=code&redirect_uri={redirect_uri_hint}&approval_prompt=force&scope=read,activity:read_all\n"
                error_msg += "2. Copy ONLY the 'code=' value from the redirect URL (without &scope=...)\n"
                error_msg += "3. Update STRAVA_AUTH_CODE in GitHub Secrets immediately\n"
                if not redirect_uri:
                    error_msg += "4. Make sure STRAVA_REDIRECT_URI in Secrets matches the redirect URI in your Strava app settings\n"
        
        raise RuntimeError(error_msg) from e
    return response.json()


def _safe_log_token(token: str, label: str = "Token") -> None:
    """Log token safely (first 8 and last 4 chars only)"""
    if token and len(token) > 12:
        masked = f"{token[:8]}...{token[-4:]}"
        print(f"DEBUG: {label}: {masked}")
    elif token:
        print(f"DEBUG: {label}: [REDACTED]")


def _save_refresh_token_to_file(token: str, filename: str = "new_refresh_token.txt") -> None:
    """Save new refresh token to file for easy retrieval"""
    try:
        with open(filename, "w") as f:
            f.write(f"# New Strava Refresh Token\n")
            f.write(f"# Copy this value to STRAVA_REFRESH_TOKEN in GitHub Secrets\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n\n")
            f.write(token)
        print(f"DEBUG: New refresh token saved to {filename} (download from artifacts)")
    except Exception as e:
        print(f"DEBUG: Could not save refresh token to file: {e}")


def get_access_token() -> str:
    # Allow overriding with a direct access token (useful for quick testing)
    direct = os.environ.get("STRAVA_ACCESS_TOKEN")
    if direct:
        return direct

    client_id = env("STRAVA_CLIENT_ID")
    client_secret = env("STRAVA_CLIENT_SECRET")

    # Try refresh token first (from DB or env); fallback to STRAVA_AUTH_CODE only if refresh fails
    store = TokenStore.from_env()
    refresh_token = None
    if store:
        try:
            refresh_token = store.load_refresh_token()
        except Exception:
            refresh_token = None
    if not refresh_token:
        env_refresh = os.environ.get("STRAVA_REFRESH_TOKEN")
        if env_refresh:
            refresh_token = env_refresh

    # If we have a refresh token, try it first (with retry)
    if refresh_token:
        url = "https://www.strava.com/oauth/token"
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        # Retry once in case of transient errors
        for attempt in range(2):
            response = requests.post(url, data=payload, timeout=30)
            if response.status_code == 200:
                # Success: use the refreshed token
                data = response.json()
                # Persist rotated refresh token if present
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
                        print("DEBUG: New refresh token saved to database.")
                    else:
                        # No DB: log the new refresh token so user can update Secret
                        _safe_log_token(new_refresh, "NEW_REFRESH_TOKEN")
                        _save_refresh_token_to_file(new_refresh)
                        print("DEBUG: ⚠️  IMPORTANT: Update STRAVA_REFRESH_TOKEN in GitHub Secrets with the new token above!")
                    # If refresh token changed, update for next attempt
                    if new_refresh != refresh_token:
                        refresh_token = new_refresh
                return data["access_token"]
            elif response.status_code == 401 and attempt == 0:
                # 401 on first attempt: token might be invalid, try once more then fallback
                print(f"DEBUG: Refresh token exchange failed (401), attempt {attempt + 1}/2")
                time.sleep(1)  # Brief pause before retry
                continue
            else:
                # Other error or second attempt failed
                print(f"DEBUG: Refresh token exchange failed with status {response.status_code}")
                break
        # If refresh token failed after retries, fall through to try auth code if available

    # No refresh token or refresh failed: try STRAVA_AUTH_CODE as fallback
    auth_code = os.environ.get("STRAVA_AUTH_CODE")
    if auth_code:
        print("DEBUG: Attempting to exchange authorization code for tokens (refresh token unavailable or invalid)...")
        try:
            data = exchange_authorization_code(client_id, client_secret, auth_code)
            new_refresh = data.get("refresh_token")
            access_token = data.get("access_token")
            if not access_token:
                raise RuntimeError("Failed to obtain access token from authorization code exchange.")
            
            # Always save new refresh token if we got one
            if new_refresh:
                if store:
                    expires_at = int(datetime.now().timestamp()) + int(data.get("expires_in") or 0)
                    store.save_tokens(
                        access_token=access_token,
                        refresh_token=new_refresh,
                        expires_at=expires_at,
                    )
                    print("DEBUG: New refresh token from auth code saved to database.")
                else:
                    # No DB: log the new refresh token so user can update Secret
                    _safe_log_token(new_refresh, "NEW_REFRESH_TOKEN")
                    _save_refresh_token_to_file(new_refresh)
                    print("DEBUG: ⚠️  IMPORTANT: Copy the NEW_REFRESH_TOKEN above and update STRAVA_REFRESH_TOKEN in GitHub Secrets!")
                    print("DEBUG: After updating, you can remove STRAVA_AUTH_CODE from Secrets.")
            print("DEBUG: Successfully obtained access token from authorization code exchange.")
            return access_token
        except RuntimeError as e:
            # Re-raise RuntimeError from exchange_authorization_code as-is (already has detailed message)
            raise
        except Exception as e:
            # For other exceptions, provide generic error
            error_msg = f"Authorization code exchange failed: {str(e)}"
            raise RuntimeError(error_msg) from e
    else:
        # No refresh token and no auth code
        error_msg = "❌ Authentication failed: No valid refresh token and no STRAVA_AUTH_CODE provided.\n\n"
        error_msg += "To fix this:\n"
        error_msg += "1. Generate a new authorization code:\n"
        error_msg += f"   https://www.strava.com/oauth/authorize?client_id={client_id}&response_type=code&redirect_uri=YOUR_REDIRECT_URI&approval_prompt=force&scope=read,activity:read_all\n"
        error_msg += "2. Set STRAVA_AUTH_CODE in GitHub Secrets (one-time bootstrap)\n"
        error_msg += "3. After successful run, update STRAVA_REFRESH_TOKEN with the new token from logs\n"
        if refresh_token:
            error_msg += f"\nNote: Existing refresh token failed (status {response.status_code if 'response' in locals() else 'unknown'})"
        raise RuntimeError(error_msg)


class TokenStore:
    def __init__(self, dsn: str):
        self.dsn = dsn
        try:
            self._ensure_table()
        except Exception:
            # If the DB isn't reachable, signal unusable store
            raise

    @staticmethod
    def from_env():
        # Allow disabling via flag
        if os.environ.get("DISABLE_DB_TOKEN_STORE") in ("1", "true", "True"):
            return None
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
        try:
            return TokenStore(dsn)
        except Exception:
            # If connection fails, gracefully degrade to no store
            return None

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
            # Attempt a single token refresh/rebootstrap, then retry once
            debug_log("401 ontvangen; probeer token te verversen en opnieuw.", log_path)
            new_token = get_access_token()
            headers = {"Authorization": f"Bearer {new_token}"}
            resp = requests.get(
                "https://www.strava.com/api/v3/athlete/activities",
                headers=headers,
                params=params,
                timeout=60,
            )
            if resp.status_code == 401:
                raise RuntimeError(
                    "401 Unauthorized fetching activities na verversen. Controleer STRAVA_REFRESH_TOKEN of gebruik STRAVA_AUTH_CODE om te bootstrappen."
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


