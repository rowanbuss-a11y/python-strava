#!/usr/bin/env python3
"""
garmin_sync.py — Incrementele Garmin Connect -> Supabase sync (single-user)

Voor JOUW eigen Garmin-account. Draait in GitHub Actions, logt in met een
opgeslagen garth-token (geen wachtwoord opgeslagen) en schrijft activiteiten
direct in je bestaande `strava_activities`-tabel, getagd met source='garmin'
en jouw user_id. Het dashboard kan zo tussen Strava en Garmin schakelen.

Vereiste env vars:
    GARMIN_TOKENS            base64-encoded garth token (zie garmin_setup.py)
    SUPABASE_URL            je Supabase project URL
    SUPABASE_KEY            service-role key (zelfde als je Strava-sync gebruikt)
    OWNER_USER_ID           jouw Supabase auth user-id (uuid)
Optioneel:
    DAYS_BACK              eerste sync hoever terug (default 365)
    FORCE_FULL_SYNC        "true" om incrementeel over te slaan
"""

import base64
import os
import sys
from datetime import datetime, timedelta, timezone

try:
    from garminconnect import Garmin
except ImportError:
    print("garminconnect niet geinstalleerd -- pip install garminconnect")
    sys.exit(1)

try:
    from supabase import create_client, Client
except ImportError:
    print("supabase-py niet geinstalleerd -- pip install supabase")
    sys.exit(1)

try:
    import polyline as _polyline
except ImportError:
    _polyline = None
    print("polyline niet geinstalleerd -- GPS-routes worden overgeslagen (pip install polyline)")

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")
GARMIN_TOKENS = os.getenv("GARMIN_TOKENS")
OWNER_USER_ID = os.getenv("OWNER_USER_ID")
DAYS_BACK     = int(os.getenv("DAYS_BACK", "365"))
SUPABASE_TABLE = "strava_activities"   # gedeelde tabel, getagd via `source`

required = {
    "SUPABASE_URL": SUPABASE_URL,
    "SUPABASE_KEY": SUPABASE_KEY,
    "GARMIN_TOKENS": GARMIN_TOKENS,
    "OWNER_USER_ID": OWNER_USER_ID,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"Ontbrekende env vars: {missing}")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Garmin typeKey -> Strava-stijl activity_type (zodat dashboard-filters werken)
TYPE_MAP = {
    "running": "Run", "trail_running": "Run", "treadmill_running": "Run",
    "track_running": "Run", "indoor_running": "Run",
    "cycling": "Ride", "road_biking": "Ride", "mountain_biking": "Ride",
    "indoor_cycling": "Ride", "gravel_cycling": "Ride", "virtual_ride": "Ride",
    "lap_swimming": "Swim", "open_water_swimming": "Swim",
    "walking": "Walk", "casual_walking": "Walk", "speed_walking": "Walk",
    "hiking": "Hike",
    "strength_training": "WeightTraining", "indoor_cardio": "Workout",
    "yoga": "Yoga", "pilates": "Workout",
}


# ── Garmin auth ───────────────────────────────────────────────────────────────
def connect_garmin() -> Garmin:
    token_data = base64.b64decode(GARMIN_TOKENS.encode()).decode()
    client = Garmin()
    client.client.loads(token_data)
    try:
        name = client.get_full_name()
    except Exception:
        name = "onbekend"
    print(f"Ingelogd bij Garmin als {name} (via token)")
    return client


# ── Supabase helpers ──────────────────────────────────────────────────────────
def get_last_garmin_date():
    if os.getenv("FORCE_FULL_SYNC", "").lower() == "true":
        print("FORCE_FULL_SYNC -- volledige sync")
        return None
    try:
        res = (
            supabase.table(SUPABASE_TABLE)
            .select("start_date")
            .eq("user_id", OWNER_USER_ID)
            .eq("source", "garmin")
            .order("start_date", desc=True)
            .limit(1)
            .execute()
        )
        if res.data:
            dt = datetime.fromisoformat(res.data[0]["start_date"].replace("Z", "+00:00"))
            print(f"Laatste Garmin-activiteit in Supabase: {dt.date()}")
            return dt
    except Exception as e:
        print(f"Kon laatste Garmin-datum niet ophalen: {e}")
    return None


def upload_rows(rows):
    if not rows:
        print("Niets te uploaden.")
        return
    BATCH = 50
    total = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        try:
            supabase.table(SUPABASE_TABLE).upsert(batch, on_conflict="id").execute()
            total += len(batch)
        except Exception as e:
            print(f"Upload fout (batch {i}): {e}")
    print(f"{total} Garmin-activiteiten geupload")


# ── Mapping ───────────────────────────────────────────────────────────────────
def _num(v):
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _int(v):
    n = _num(v)
    return int(n) if n is not None else None


def _parse_gmt(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return s


def map_activity(a: dict) -> dict:
    type_key = (a.get("activityType") or {}).get("typeKey") or ""
    activity_type = TYPE_MAP.get(type_key, type_key.replace("_", " ").title() or "Workout")
    moving = a.get("movingDuration") or a.get("duration")
    return {
        "id":                   _int(a.get("activityId")),
        "user_id":              OWNER_USER_ID,
        "source":               "garmin",
        "name":                 a.get("activityName") or "Garmin activiteit",
        "type":                 activity_type,
        "activity_type":        activity_type,
        "distance":             _num(a.get("distance")),
        "moving_time":          _int(moving),
        "elapsed_time":         _int(a.get("elapsedDuration") or a.get("duration")),
        "total_elevation_gain": _num(a.get("elevationGain")),
        "start_date":           _parse_gmt(a.get("startTimeGMT")),
        "average_speed":        _num(a.get("averageSpeed")),
        "max_speed":            _num(a.get("maxSpeed")),
        "average_heartrate":    _num(a.get("averageHR")),
        "max_heartrate":        _num(a.get("maxHR")),
        "calories":             _num(a.get("calories")),
        "description":          None,
        "kudos_count":          0,
        "comment_count":        0,
        "gear_name":            None,
        "map_summary_polyline": None,
        "external_id":          f"garmin-{a.get('activityId')}",
    }


# ── GPS ───────────────────────────────────────────────────────────────────────
def fetch_detail(client, activity_id, distance):
    """Haalt de GPS-track op uit één detail-call.

    Retourneert (polyline | None, points), waarbij points een lijst is van
    (time_offset_seconden, lat, lon). Leeg voor indoor-activiteiten of zonder GPS.
    Faalt stil: ontbrekende GPS mag de sync nooit blokkeren.
    """
    if not distance or distance <= 0:
        return None, []
    try:
        det = client.get_activity_details(activity_id, maxchart=2000, maxpoly=2000)
        pts = (det.get("geoPolylineDTO") or {}).get("polyline") or []
        clean = [p for p in pts if p.get("lat") is not None and p.get("lon") is not None]
        poly = None
        if _polyline is not None and len(clean) >= 2:
            poly = _polyline.encode([(p["lat"], p["lon"]) for p in clean])
        points = []
        if clean:
            t0 = clean[0].get("time") or 0
            for p in clean:
                t = p.get("time")
                off = int((t - t0) / 1000) if t is not None else None
                points.append((off, p["lat"], p["lon"]))
        return poly, points
    except Exception as e:
        print(f"  detail ophalen mislukt voor {activity_id}: {e}")
        return None, []


def fetch_splits(client, activity_id):
    """Garmin's eigen per-km auto-laps -> [{km, pace(min/km)}]; exacter dan GPS."""
    try:
        laps = (client.get_activity_splits(activity_id) or {}).get("lapDTOs") or []
        out = []
        for i, l in enumerate(laps):
            d = l.get("distance") or 0
            sec = l.get("duration") or 0
            if d < 400 or sec <= 0:  # sla de partiële rest-lap over
                continue
            hr = l.get("averageHR")
            gain = l.get("elevationGain") or 0
            loss = l.get("elevationLoss") or 0
            out.append({"km": i + 1, "pace": round((sec / 60) * (1000 / d), 4),
                        "hr": round(hr) if hr else None,
                        "elev": round(gain - loss)})
        return out or None
    except Exception as e:
        print(f"  splits ophalen mislukt voor {activity_id}: {e}")
        return None


def write_gps_points(activity_id, name, atype, points):
    """Vervangt de GPS-punten van een activiteit in strava_gps_points."""
    try:
        supabase.table("strava_gps_points").delete().eq("activity_id", activity_id).execute()
        rows = [{
            "activity_id": activity_id, "activity_name": name, "activity_type": atype,
            "latitude": lat, "longitude": lon, "time_offset": off,
        } for (off, lat, lon) in points if off is not None]
        for i in range(0, len(rows), 500):
            supabase.table("strava_gps_points").insert(rows[i:i + 500]).execute()
        print(f"  {len(rows)} GPS-punten opgeslagen voor {activity_id}")
    except Exception as e:
        print(f"  GPS-punten opslaan mislukt voor {activity_id}: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("Garmin -> Supabase sync gestart (single-user)")
    client = connect_garmin()

    last = get_last_garmin_date()
    if last:
        start_date = (last - timedelta(hours=1)).date()
        print(f"Incrementele sync vanaf {start_date}")
    else:
        start_date = (datetime.utcnow() - timedelta(days=DAYS_BACK)).date()
        print(f"Eerste sync vanaf {start_date}")
    end_date = datetime.utcnow().date()

    try:
        raw = client.get_activities_by_date(start_date.isoformat(), end_date.isoformat())
    except Exception as e:
        print(f"Ophalen mislukt: {e}")
        sys.exit(1)

    print(f"{len(raw)} activiteiten ontvangen van Garmin")

    rows = []
    for a in raw:
        try:
            row = map_activity(a)
            if row["id"] is not None and row["start_date"]:
                poly, points = fetch_detail(client, a.get("activityId"), row.get("distance"))
                row["map_summary_polyline"] = poly
                if row["type"] == "Run":
                    if points:
                        write_gps_points(row["id"], row["name"], row["type"], points)
                    row["splits_data"] = fetch_splits(client, a.get("activityId"))
                rows.append(row)
        except Exception as e:
            print(f"Mapping-fout bij {a.get('activityId')}: {e}")

    upload_rows(rows)
    print(f"Klaar -- {len(rows)} Garmin-activiteiten verwerkt")


if __name__ == "__main__":
    main()
