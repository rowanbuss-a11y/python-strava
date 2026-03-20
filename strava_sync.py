#!/usr/bin/env python3
"""
strava_sync.py — Geoptimaliseerde Strava → Supabase sync

Verbeteringen t.o.v. vorige versie:
- 15-minuten window bewaking (100 calls/15min hard limit van Strava)
- Slimmere sleep: adaptief op basis van window gebruik
- Splits sync verwijderd
- Dagelijks budget bewaking (1000 calls/dag)
- Betere logging van rate limit status
"""

import os
import time
import json
import csv
import requests
from datetime import datetime, timedelta, timezone
from collections import deque
from supabase import create_client, Client

# -----------------------
# Config / env
# -----------------------
SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_KEY         = os.getenv("SUPABASE_KEY")
STRAVA_CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")

CSV_FILE  = os.getenv("CSV_FILE",  "activiteiten.csv")
JSON_FILE = os.getenv("JSON_FILE", "activiteiten_raw.json")
DAYS_BACK = int(os.getenv("DAYS_BACK", "365"))
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "strava_activities")

# Rate limit instellingen (Strava limieten)
RATE_LIMIT_15MIN  = 90   # gebruik 90 van de 100 als veiligheidsmarge
RATE_LIMIT_DAILY  = 950  # gebruik 950 van de 1000 als veiligheidsmarge
MIN_SLEEP_BETWEEN_CALLS = 1.0  # minimale pauze tussen API calls (seconden)

required_env = {
    "SUPABASE_URL": SUPABASE_URL, "SUPABASE_KEY": SUPABASE_KEY,
    "STRAVA_CLIENT_ID": STRAVA_CLIENT_ID,
    "STRAVA_CLIENT_SECRET": STRAVA_CLIENT_SECRET,
    "STRAVA_REFRESH_TOKEN": STRAVA_REFRESH_TOKEN,
}
missing = [k for k, v in required_env.items() if not v]
if missing:
    raise RuntimeError(f"Missing required env vars: {missing}")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------
# Rate limit tracker
# -----------------------
class RateLimiter:
    """
    Bewaakt de Strava rate limits:
    - 15-minuten window: max 100 calls (we gebruiken 90)
    - Dagelijks: max 1000 calls (we gebruiken 950)

    Strava stuurt X-RateLimit-Limit en X-RateLimit-Usage headers mee.
    We lezen die uit en wachten proactief als we dicht bij de grens komen.
    """
    def __init__(self):
        self.call_times_15min = deque()  # timestamps van calls in afgelopen 15 min
        self.daily_count = 0
        self.start_time = datetime.now()

    def _clean_window(self):
        """Verwijder calls ouder dan 15 minuten uit de window."""
        cutoff = time.time() - 900  # 15 minuten = 900 seconden
        while self.call_times_15min and self.call_times_15min[0] < cutoff:
            self.call_times_15min.popleft()

    def check_and_wait(self):
        """Wacht indien nodig om binnen rate limits te blijven."""
        self._clean_window()

        # Dagelijks budget check
        if self.daily_count >= RATE_LIMIT_DAILY:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            print(f"🛑 Dagelijks budget bereikt ({self.daily_count} calls). Sync gestopt.")
            raise RuntimeError("Daily rate limit reached")

        # 15-minuten window check
        calls_in_window = len(self.call_times_15min)
        if calls_in_window >= RATE_LIMIT_15MIN:
            # Wacht tot de oudste call uit het window valt
            oldest = self.call_times_15min[0]
            wait_until = oldest + 900
            wait_secs = max(0, wait_until - time.time())
            if wait_secs > 0:
                print(f"⏳ 15-min window vol ({calls_in_window}/{RATE_LIMIT_15MIN}) — wacht {wait_secs:.0f}s")
                time.sleep(wait_secs + 2)  # +2s extra marge
            self._clean_window()

        # Minimale pauze altijd aanhouden
        time.sleep(MIN_SLEEP_BETWEEN_CALLS)

    def register_call(self, response=None):
        """Registreer een API call en lees eventuele rate limit headers uit."""
        now = time.time()
        self.call_times_15min.append(now)
        self.daily_count += 1

        # Lees Strava rate limit headers als beschikbaar
        if response is not None:
            usage = response.headers.get("X-RateLimit-Usage", "")
            limit = response.headers.get("X-RateLimit-Limit", "")
            if usage and limit:
                parts = usage.split(",")
                lparts = limit.split(",")
                if len(parts) >= 2 and len(lparts) >= 2:
                    used_15 = int(parts[0].strip())
                    used_day = int(parts[1].strip())
                    lim_15  = int(lparts[0].strip())
                    lim_day = int(lparts[1].strip())
                    # Waarschuw als we dicht bij de grens komen
                    if used_15 >= lim_15 * 0.85:
                        print(f"  ⚠️ 15-min usage: {used_15}/{lim_15}")
                    if used_day >= lim_day * 0.85:
                        print(f"  ⚠️ Dagelijks usage: {used_day}/{lim_day}")

    def status(self):
        self._clean_window()
        return f"calls: {len(self.call_times_15min)}/15min, {self.daily_count}/dag"

# Globale rate limiter
limiter = RateLimiter()

# -----------------------
# HTTP helpers
# -----------------------
def safe_get(url, headers=None, params=None, max_retries=8, backoff=2):
    headers = headers or {}
    attempt = 0
    while attempt < max_retries:
        limiter.check_and_wait()
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
        except requests.RequestException as e:
            attempt += 1
            wait = backoff ** attempt
            print(f"⚠️ RequestException: {e} — retry in {wait}s")
            time.sleep(wait)
            continue

        limiter.register_call(r)

        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and str(retry_after).isdigit() else 60 * (attempt + 1)
            print(f"⏳ 429 rate limit — wacht {wait}s")
            time.sleep(wait)
            attempt += 1
            continue

        return r

    raise RuntimeError(f"Max retries reached voor GET {url}")

def safe_post(url, headers=None, data=None, json_payload=None, max_retries=3):
    headers = headers or {}
    attempt = 0
    while attempt < max_retries:
        try:
            r = requests.post(url, headers=headers, data=data, json=json_payload, timeout=30)
        except requests.RequestException as e:
            attempt += 1
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and str(retry_after).isdigit() else 5
            time.sleep(wait)
            attempt += 1
            continue
        return r
    raise RuntimeError(f"Max retries reached voor POST {url}")

# -----------------------
# Strava auth & fetch
# -----------------------
def refresh_access_token():
    r = safe_post("https://www.strava.com/api/v3/oauth/token", data={
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": STRAVA_REFRESH_TOKEN,
    })
    r.raise_for_status()
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError("Geen access token ontvangen")
    print("✅ Access token opgehaald")
    return token

def fetch_activities_summary(access_token, after_ts):
    print(f"⏱ Ophalen activiteiten vanaf {datetime.utcfromtimestamp(after_ts).date()}")
    all_acts = []
    page = 1
    headers = {"Authorization": f"Bearer {access_token}"}
    while True:
        r = safe_get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers=headers,
            params={"page": page, "per_page": 200, "after": after_ts}
        )
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        all_acts.extend(data)
        print(f"  → pagina {page}: {len(data)} activiteiten ({limiter.status()})")
        if len(data) < 200:
            break
        page += 1
    print(f"📦 {len(all_acts)} activiteiten opgehaald")
    return all_acts

def fetch_activity_details(access_token, activity_id):
    headers = {"Authorization": f"Bearer {access_token}"}
    r = safe_get(f"https://www.strava.com/api/v3/activities/{activity_id}", headers=headers)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()

def fetch_gear_name(access_token, gear_id):
    if not gear_id:
        return None
    headers = {"Authorization": f"Bearer {access_token}"}
    r = safe_get(f"https://www.strava.com/api/v3/gear/{gear_id}", headers=headers)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json().get("name")

# -----------------------
# Supabase helpers
# -----------------------
def ensure_supabase_columns():
    print("🔍 Supabase kolommen controleren...")
    required_columns = {
        "id": "bigint", "name": "text", "type": "text", "activity_type": "text",
        "start_date": "timestamptz", "start_date_local": "text",
        "distance": "numeric", "distance_km": "numeric",
        "moving_time": "integer", "elapsed_time": "integer",
        "total_elevation_gain": "numeric", "elev_high": "numeric", "elev_low": "numeric",
        "average_speed": "numeric", "average_speed_kmh": "numeric",
        "max_speed": "numeric", "max_speed_kmh": "numeric",
        "average_watts": "numeric", "weighted_average_watts": "numeric",
        "kilojoules": "numeric", "calories": "numeric", "suffer_score": "numeric",
        "average_heartrate": "numeric", "max_heartrate": "numeric",
        "has_heartrate": "boolean", "pr_count": "integer",
        "kudos_count": "integer", "comment_count": "integer",
        "athlete_count": "integer", "photo_count": "integer",
        "gear_id": "text", "gear_name": "text", "device_name": "text",
        "perceived_exertion": "integer", "workout_type": "integer",
        "achievement_count": "integer", "trainer": "boolean",
        "commute": "boolean", "private": "boolean", "flagged": "boolean",
        "best_efforts_count": "integer", "splits_metric_count": "integer",
        "laps_count": "integer", "segment_efforts_count": "integer",
        "map_id": "text", "map_summary_polyline": "text",
        "map_polyline": "text", "map_resource_state": "integer",
        "external_id": "text", "upload_id": "bigint", "description": "text",
    }
    for col, col_type in required_columns.items():
        try:
            supabase.rpc("sql", {"query": f"ALTER TABLE {SUPABASE_TABLE} ADD COLUMN IF NOT EXISTS {col} {col_type};"})
        except Exception:
            pass
    time.sleep(1)
    print("✅ Kolommen gecontroleerd")

def get_last_activity_date():
    if os.getenv("FORCE_FULL_SYNC", "").lower() == "true":
        print("⚠️ FORCE_FULL_SYNC — volledige sync")
        return None
    try:
        result = supabase.table(SUPABASE_TABLE)\
            .select("start_date").order("start_date", desc=True).limit(1).execute()
        if result.data:
            dt = datetime.fromisoformat(result.data[0]["start_date"].replace("Z", "+00:00"))
            print(f"📅 Laatste in Supabase: {dt.date()}")
            return dt
    except Exception as e:
        print(f"⚠️ Kon laatste datum niet ophalen: {e}")
    return None

# -----------------------
# Data verwerking
# -----------------------
def safe_num(x):
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except Exception:
        return None

def safe_int(x):
    if x is None or isinstance(x, bool):
        return None
    try:
        return int(float(x))
    except Exception:
        return None

def prepare_row(summary, details, access_token, gear_cache):
    d = details or {}
    s = summary or {}

    # Gear ophalen met cache
    gear_id = d.get("gear_id") or s.get("gear_id")
    gear_name = None
    if isinstance(d.get("gear"), dict):
        gear_name = d["gear"].get("name")
        gear_id = gear_id or d["gear"].get("id")
    if not gear_name and gear_id:
        if gear_id in gear_cache:
            gear_name = gear_cache[gear_id]
        else:
            try:
                gear_name = fetch_gear_name(access_token, gear_id)
            except Exception:
                gear_name = None
            gear_cache[gear_id] = gear_name

    dist      = d.get("distance",      s.get("distance"))
    avg_speed = d.get("average_speed", s.get("average_speed"))
    max_speed = d.get("max_speed",     s.get("max_speed"))
    activity_type = d.get("sport_type") or d.get("type") or s.get("sport_type") or s.get("type")

    return {
        "id":                     safe_int(d.get("id") or s.get("id")),
        "name":                   d.get("name") or s.get("name"),
        "type":                   d.get("type") or s.get("type"),
        "activity_type":          activity_type,
        "start_date":             d.get("start_date") or s.get("start_date"),
        "start_date_local":       d.get("start_date_local") or s.get("start_date_local"),
        "distance":               safe_num(dist),
        "distance_km":            round(safe_num(dist) / 1000, 3) if safe_num(dist) else None,
        "moving_time":            safe_int(d.get("moving_time",        s.get("moving_time"))),
        "elapsed_time":           safe_int(d.get("elapsed_time",       s.get("elapsed_time"))),
        "total_elevation_gain":   safe_num(d.get("total_elevation_gain", s.get("total_elevation_gain"))),
        "elev_high":              safe_num(d.get("elev_high")),
        "elev_low":               safe_num(d.get("elev_low")),
        "average_speed":          safe_num(avg_speed),
        "average_speed_kmh":      round(safe_num(avg_speed) * 3.6, 3) if safe_num(avg_speed) else None,
        "max_speed":              safe_num(max_speed),
        "max_speed_kmh":          round(safe_num(max_speed) * 3.6, 3) if safe_num(max_speed) else None,
        "average_watts":          safe_num(d.get("average_watts")),
        "weighted_average_watts": safe_num(d.get("weighted_average_watts")),
        "kilojoules":             safe_num(d.get("kilojoules")),
        "calories":               safe_num(d.get("calories")),
        "suffer_score":           safe_num(d.get("suffer_score")),
        "average_heartrate":      safe_num(d.get("average_heartrate")),
        "max_heartrate":          safe_num(d.get("max_heartrate")),
        "has_heartrate":          bool(d.get("has_heartrate", False)),
        "pr_count":               safe_int(d.get("pr_count", 0)),
        "kudos_count":            safe_int(d.get("kudos_count") or s.get("kudos_count") or 0),
        "comment_count":          safe_int(d.get("comment_count") or s.get("comment_count") or 0),
        "athlete_count":          safe_int(d.get("athlete_count") or s.get("athlete_count") or 1),
        "photo_count":            safe_int(d.get("photo_count") or s.get("photo_count") or 0),
        "gear_id":                gear_id,
        "gear_name":              gear_name,
        "device_name":            d.get("device_name") or s.get("device_name"),
        "perceived_exertion":     safe_int(d.get("perceived_exertion")),
        "workout_type":           safe_int(d.get("workout_type")),
        "achievement_count":      safe_int(d.get("achievement_count", 0)),
        "trainer":                bool(d.get("trainer", False)),
        "commute":                bool(d.get("commute", False)),
        "private":                bool(d.get("private", False)),
        "flagged":                bool(d.get("flagged", False)),
        "best_efforts_count":     safe_int(len(d.get("best_efforts") or [])),
        "splits_metric_count":    safe_int(len(d.get("splits_metric") or [])),
        "laps_count":             safe_int(len(d.get("laps") or [])),
        "segment_efforts_count":  safe_int(len(d.get("segment_efforts") or [])),
        "map_id":                 (d.get("map") or s.get("map") or {}).get("id"),
        "map_summary_polyline":   (d.get("map") or s.get("map") or {}).get("summary_polyline"),
        "map_polyline":           (d.get("map") or s.get("map") or {}).get("polyline"),
        "map_resource_state":     safe_int((d.get("map") or s.get("map") or {}).get("resource_state")),
        "external_id":            d.get("external_id") or s.get("external_id"),
        "upload_id":              d.get("upload_id") or s.get("upload_id"),
        "description":            d.get("description") or s.get("description"),
    }

# -----------------------
# Upload + backup
# -----------------------
def upload_rows(rows):
    if not rows:
        print("ℹ️ Niets te uploaden.")
        return
    # Batch per 50 voor stabiele uploads
    BATCH = 50
    total = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        try:
            supabase.table(SUPABASE_TABLE).upsert(batch, on_conflict="id").execute()
            total += len(batch)
        except Exception as e:
            print(f"❌ Upload error (batch {i}): {e}")
    print(f"✅ {total} rijen geüpload naar Supabase")

def save_json_csv(rows):
    if not rows:
        return
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"💾 Backup opgeslagen: {JSON_FILE} + {CSV_FILE}")

# -----------------------
# Main
# -----------------------
def main():
    print("▶️ Strava → Supabase sync gestart")
    print(f"   Rate limits: {RATE_LIMIT_15MIN}/15min, {RATE_LIMIT_DAILY}/dag")
    print()

    ensure_supabase_columns()
    access_token = refresh_access_token()

    # Bepaal startpunt
    last_date = get_last_activity_date()
    if last_date:
        after_dt = last_date - timedelta(hours=1)
        print(f"🔄 Incrementele sync vanaf {after_dt.date()}")
    else:
        after_dt = datetime.utcnow() - timedelta(days=DAYS_BACK)
        print(f"🆕 Eerste sync vanaf {after_dt.date()}")

    after_ts = int(after_dt.replace(tzinfo=timezone.utc).timestamp())
    summaries = fetch_activities_summary(access_token, after_ts)

    if not summaries:
        print("✅ Geen nieuwe activiteiten — Supabase is up-to-date.")
        return

    # Resterende dagelijkse calls berekenen
    remaining_calls = RATE_LIMIT_DAILY - limiter.daily_count
    max_details = remaining_calls - 5  # 5 calls reserve
    if len(summaries) > max_details:
        print(f"⚠️ Budget: {max_details} detail-calls beschikbaar, {len(summaries)} activiteiten gevonden")
        print(f"   Verwerken van de {max_details} meest recente — rest volgende run")
        summaries = summaries[:max_details]

    rows = []
    gear_cache = {}
    for idx, s in enumerate(summaries, start=1):
        aid = s.get("id")
        print(f"[{idx}/{len(summaries)}] activiteit {aid} ... ({limiter.status()})")
        try:
            details = fetch_activity_details(access_token, aid)
            row = prepare_row(s, details, access_token, gear_cache)
            rows.append(row)
        except RuntimeError as e:
            if "Daily rate limit" in str(e):
                print(f"🛑 Dagelijks budget bereikt bij activiteit {idx} — tussenresultaten opslaan")
                break
            print(f"⚠️ Fout bij {aid}: {e}")
        except Exception as e:
            print(f"⚠️ Fout bij {aid}: {e}")

    upload_rows(rows)
    save_json_csv(rows)
    print(f"\n✅ Sync klaar — {len(rows)} activiteiten verwerkt")
    print(f"   Totaal API calls deze run: {limiter.daily_count}")

if __name__ == "__main__":
    main()
