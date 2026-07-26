#!/usr/bin/env python3
"""
garmin_sync.py — Incrementele Garmin Connect -> Supabase sync (multi-user)

Synct iedere gebruiker die een Garmin-token heeft opgeslagen én Garmin als
kanaal heeft gekozen. Tokens staan per gebruiker in de tabel `garmin_tokens`;
de keuze staat in `user_sync_settings.channel`. Activiteiten komen in de
gedeelde tabel `strava_activities`, getagd met source='garmin' + de user_id.

Er worden nooit Garmin-wachtwoorden opgeslagen: `garmin_setup.py` levert een
sessietoken en alleen dat token gaat de database in.

Vereiste env vars:
    SUPABASE_URL            je Supabase project URL
    SUPABASE_KEY            service-role key
Optioneel:
    ONLY_USER_ID           sync alleen deze gebruiker (gebruikt door de app-knop)
    DAYS_BACK              eerste sync hoever terug (default 365)
    FORCE_FULL_SYNC        "true" om incrementeel over te slaan
    GARMIN_TOKENS/OWNER_USER_ID  legacy: eenmalige migratie naar garmin_tokens
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
# Tokens komen uit de tabel garmin_tokens (per gebruiker). De env-vars hieronder
# bestaan alleen nog als eenmalige migratieroute voor de oorspronkelijke setup.
LEGACY_TOKENS = os.getenv("GARMIN_TOKENS")
LEGACY_USER   = os.getenv("OWNER_USER_ID")
ONLY_USER     = os.getenv("ONLY_USER_ID")  # optioneel: sync één specifieke gebruiker
DAYS_BACK     = int(os.getenv("DAYS_BACK", "365"))
SUPABASE_TABLE = "strava_activities"   # gedeelde tabel, getagd via `source`

required = {
    "SUPABASE_URL": SUPABASE_URL,
    "SUPABASE_KEY": SUPABASE_KEY,
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
def connect_garmin(token_b64: str) -> Garmin:
    token_data = base64.b64decode(token_b64.encode()).decode()
    client = Garmin()
    client.client.loads(token_data)
    try:
        name = client.get_full_name()
    except Exception:
        name = "onbekend"
    print(f"Ingelogd bij Garmin als {name} (via token)")
    return client


# ── Supabase helpers ──────────────────────────────────────────────────────────
def get_last_garmin_date(user_id: str):
    if os.getenv("FORCE_FULL_SYNC", "").lower() == "true":
        print("FORCE_FULL_SYNC -- volledige sync")
        return None
    try:
        res = (
            supabase.table(SUPABASE_TABLE)
            .select("start_date")
            .eq("user_id", user_id)
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


def map_activity(a: dict, user_id: str) -> dict:
    type_key = (a.get("activityType") or {}).get("typeKey") or ""
    activity_type = TYPE_MAP.get(type_key, type_key.replace("_", " ").title() or "Workout")
    moving = a.get("movingDuration") or a.get("duration")
    return {
        "id":                   _int(a.get("activityId")),
        "user_id":              user_id,
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
        # map_summary_polyline en splits_data moeten ALTIJD aanwezig zijn: PostgREST
        # weigert een bulk-insert waarvan de rijen niet exact dezelfde velden hebben
        # ("All object keys must match"). Een run mét splits naast een krachtsessie
        # zónder splits liet daardoor de hele batch mislukken.
        "map_summary_polyline": None,
        "splits_data":          None,
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
def sync_user(user_id: str, token_b64: str) -> int:
    """Synct één gebruiker. Retourneert het aantal verwerkte activiteiten."""
    client = connect_garmin(token_b64)

    last = get_last_garmin_date(user_id)
    if last:
        start_date = (last - timedelta(hours=1)).date()
        print(f"  incrementeel vanaf {start_date}")
    else:
        start_date = (datetime.utcnow() - timedelta(days=DAYS_BACK)).date()
        print(f"  eerste sync vanaf {start_date}")
    end_date = datetime.utcnow().date()

    raw = client.get_activities_by_date(start_date.isoformat(), end_date.isoformat())
    print(f"  {len(raw)} activiteiten ontvangen van Garmin")

    rows = []
    for a in raw:
        try:
            row = map_activity(a, user_id)
            if row["id"] is None or not row["start_date"]:
                continue
            poly, points = fetch_detail(client, a.get("activityId"), row.get("distance"))
            row["map_summary_polyline"] = poly
            if row["type"] == "Run":
                if points:
                    write_gps_points(row["id"], row["name"], row["type"], points)
                row["splits_data"] = fetch_splits(client, a.get("activityId"))
            rows.append(row)
        except Exception as e:
            print(f"  mapping-fout bij {a.get('activityId')}: {e}")

    upload_rows(rows)
    return len(rows)


def load_targets():
    """Gebruikers met een Garmin-token die Garmin ook als kanaal hebben gekozen."""
    # Eenmalige migratie: een token dat nog als env var wordt aangeleverd wordt
    # alsnog in garmin_tokens gezet, zodat de oude setup blijft werken.
    if LEGACY_TOKENS and LEGACY_USER:
        try:
            supabase.table("garmin_tokens").upsert(
                {"user_id": LEGACY_USER, "token_data": LEGACY_TOKENS}, on_conflict="user_id"
            ).execute()
            print("Legacy GARMIN_TOKENS overgezet naar garmin_tokens")
        except Exception as e:
            print(f"Legacy-migratie overgeslagen: {e}")

    try:
        tokens = supabase.table("garmin_tokens").select("user_id, token_data").execute().data or []
    except Exception as e:
        print(f"Kon garmin_tokens niet lezen: {e}")
        return []

    if ONLY_USER:
        tokens = [t for t in tokens if t["user_id"] == ONLY_USER]
        return tokens

    # Alleen wie Garmin daadwerkelijk als kanaal heeft gekozen.
    try:
        prefs = supabase.table("user_sync_settings").select("user_id, channel").execute().data or []
        garmin_users = {p["user_id"] for p in prefs if p.get("channel") == "garmin"}
        # Geen expliciete keuze maar wel een token: meenemen (opt-in door te koppelen).
        known = {p["user_id"] for p in prefs}
        return [t for t in tokens if t["user_id"] in garmin_users or t["user_id"] not in known]
    except Exception as e:
        print(f"Kon kanaalkeuzes niet lezen ({e}) -- sync alle tokens")
        return tokens


def main():
    targets = load_targets()
    if not targets:
        print("Geen gebruikers met Garmin als kanaal. Klaar.")
        return

    print(f"Garmin -> Supabase sync voor {len(targets)} gebruiker(s)")
    total, failed = 0, 0
    for t in targets:
        uid = t["user_id"]
        print(f"- gebruiker {uid[:8]}...")
        try:
            total += sync_user(uid, t["token_data"])
            supabase.table("garmin_tokens").update(
                {"last_sync_at": datetime.now(timezone.utc).isoformat(), "last_error": None}
            ).eq("user_id", uid).execute()
        except Exception as e:
            failed += 1
            msg = str(e)[:300]
            print(f"  MISLUKT: {msg}")
            # Een kapot token van één gebruiker mag de rest niet blokkeren.
            try:
                supabase.table("garmin_tokens").update({"last_error": msg}).eq("user_id", uid).execute()
            except Exception:
                pass

    print(f"Klaar -- {total} activiteiten verwerkt, {failed} gebruiker(s) mislukt")
    if failed and failed == len(targets):
        sys.exit(1)


if __name__ == "__main__":
    main()
