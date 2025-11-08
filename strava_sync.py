import requests
import os
import time
import json
import csv
import polyline
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

# --- Configuratie ---
CLIENT_ID = 129018
CLIENT_SECRET = "69d0ce2fdd3cdfc33b037b5e43d3f9f3faf0eed4"
REFRESH_TOKEN = "aec3efbf4e76dcae6ec1c658c14e8620e5bfef5b"

CSV_FILE = "/Users/rowanbuss/Desktop/STRAVA NIEUW/activiteiten.csv"
JSON_FILE = "/Users/rowanbuss/Desktop/STRAVA NIEUW/activiteiten_raw.json"
GPS_FILE = "/Users/rowanbuss/Desktop/STRAVA NIEUW/strava_gps_data.csv"
EXISTING_DETAILS_FILE = "/Users/rowanbuss/Desktop/STRAVA NIEUW/existing_details.json"

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Voetbal8!',
    'host': 'db.lqpdxitcqnfbsikdbopq.supabase.co',
    'port': '5432'
}

INITIAL_RATE_LIMIT_PAUSE = 30

# --- Functies ---

def get_access_token():
    """Haal een geldig Strava access token op met refresh token"""
    global REFRESH_TOKEN
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    data = response.json()
    REFRESH_TOKEN = data["refresh_token"]  # update refresh token
    return data["access_token"]

def load_existing_details():
    if os.path.exists(EXISTING_DETAILS_FILE):
        with open(EXISTING_DETAILS_FILE, 'r') as f:
            return {str(k): v for k, v in json.load(f).items()}
    return {}

def save_existing_details(details_dict):
    with open(EXISTING_DETAILS_FILE, 'w') as f:
        json.dump(details_dict, f)

def get_existing_ids():
    existing_ids = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'ID' in row:
                    existing_ids.add(str(row['ID']))
    return existing_ids

def handle_rate_limit(response=None, pause_time=INITIAL_RATE_LIMIT_PAUSE):
    print(f"Rate limit bereikt, wachten {pause_time} sec...")
    time.sleep(pause_time)

def get_activity_details(activity_id, access_token, existing_details):
    str_id = str(activity_id)
    if str_id in existing_details:
        return existing_details[str_id]
    
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"include_all_efforts": True, "keys_by_type": True}
    
    for attempt in range(3):
        r = requests.get(url, headers=headers, params=params)
        if r.status_code == 429:
            handle_rate_limit(r)
            continue
        r.raise_for_status()
        data = r.json()
        existing_details[str_id] = data
        return data
    return None

def get_activities(access_token, existing_ids, last_date=None):
    all_activities = []
    page = 1
    per_page = 200
    consecutive_empty_pages = 0
    
    while consecutive_empty_pages < 5:
        url = "https://www.strava.com/api/v3/athlete/activities"
        params = {"page": page, "per_page": per_page}
        if last_date:
            params["after"] = int(last_date.timestamp())
        headers = {"Authorization": f"Bearer {access_token}"}
        r = requests.get(url, headers=headers, params=params)
        if r.status_code == 429:
            handle_rate_limit(r)
            continue
        r.raise_for_status()
        activities = r.json()
        if not activities:
            consecutive_empty_pages += 1
            page += 1
            continue
        new_activities = [a for a in activities if str(a['id']) not in existing_ids]
        all_activities.extend(new_activities)
        page += 1
    return all_activities

def save_activities_to_csv(activities):
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
    if not activities:
        return
    fieldnames = list(prepare_activity_row(activities[0]).keys())
    file_exists = os.path.exists(CSV_FILE)
    
    existing_ids = get_existing_ids()
    new_rows = [prepare_activity_row(a) for a in activities if str(a.get('id')) not in existing_ids]
    
    mode = 'a' if file_exists else 'w'
    with open(CSV_FILE, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)

def save_gps_data(activities):
    os.makedirs(os.path.dirname(GPS_FILE), exist_ok=True)
    with open(GPS_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['ActivityID','ActivityName','ActivityType','Latitude','Longitude','Timestamp','Distance','Elevation'])
        writer.writeheader()
        for a in activities:
            if 'map' in a and 'summary_polyline' in a['map']:
                points = polyline.decode(a['map']['summary_polyline'])
                if not points:
                    continue
                start_time = datetime.strptime(a['start_date'], "%Y-%m-%dT%H:%M:%SZ")
                duration = a.get('moving_time', 0)
                total_distance = a.get('distance', 0)
                total_elevation = a.get('total_elevation_gain', 0)
                for idx, (lat,lng) in enumerate(points):
                    t = start_time + timedelta(seconds=duration * idx / len(points))
                    writer.writerow({
                        'ActivityID': a['id'],
                        'ActivityName': a['name'],
                        'ActivityType': a['type'],
                        'Latitude': lat,
                        'Longitude': lng,
                        'Timestamp': t,
                        'Distance': total_distance / len(points),
                        'Elevation': total_elevation / len(points)
                    })

def prepare_activity_row(a):
    gear = a.get('gear', {}) if isinstance(a.get('gear'), dict) else {}
    row = {
        'ID': a.get('id'),
        'Naam': a.get('name'),
        'Datum': a.get('start_date'),
        'Type': a.get('type'),
        'Afstand (km)': round(a.get('distance',0)/1000,2),
        'Tijd (min)': round(a.get('moving_time',0)/60,2),
        'Totale tijd (min)': round(a.get('elapsed_time',0)/60,2),
        'Hoogtemeters': a.get('total_elevation_gain',0),
        'Gemiddelde snelheid (km/u)': round(a.get('average_speed',0)*3.6,2),
        'Max snelheid (km/u)': round(a.get('max_speed',0)*3.6,2),
        'Calories': a.get('calories'),
        'Gear naam': gear.get('name') if gear else None,
    }
    return row

def save_to_database(activities):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
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
            calories INTEGER,
            gear_name VARCHAR(255)
        )
    """)
    activity_data = [(a.get('id'), a.get('name'), a.get('type'), a.get('start_date'), a.get('distance'),
                      a.get('moving_time'), a.get('elapsed_time'), a.get('total_elevation_gain'),
                      a.get('average_speed'), a.get('max_speed'), a.get('calories'),
                      a.get('gear',{}).get('name') if isinstance(a.get('gear'), dict) else None) for a in activities]
    execute_values(cur, """
        INSERT INTO strava_activities (id,name,type,start_date,distance,moving_time,elapsed_time,total_elevation_gain,average_speed,max_speed,calories,gear_name)
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
            calories = EXCLUDED.calories,
            gear_name = EXCLUDED.gear_name
    """, activity_data)
    conn.commit()
    cur.close()
    conn.close()

# --- Main execution ---
def main():
    access_token = get_access_token()
    existing_ids = get_existing_ids()
    existing_details = load_existing_details()
    
    # Haal activiteiten op van de laatste 30 dagen
    last_date = datetime.now() - timedelta(days=30)
    activities_summary = get_activities(access_token, existing_ids, last_date)
    
    all_activities = []
    for a in activities_summary:
        details = get_activity_details(a['id'], access_token, existing_details)
        if details:
            merged = {**a, **details}
            all_activities.append(merged)
        else:
            all_activities.append(a)
    
    save_existing_details(existing_details)
    
    # Opslaan
    save_activities_to_csv(all_activities)
    save_gps_data(all_activities)
    save_to_database(all_activities)
    print(f"Totaal {len(all_activities)} activiteiten verwerkt.")

if __name__ == "__main__":
    main()
