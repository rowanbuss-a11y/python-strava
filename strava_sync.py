import requests
import json
import os
import time
import socket
from contextlib import closing
from datetime import datetime, timedelta
import csv
import polyline
import psycopg2
from psycopg2.extras import execute_values

# ==========================
# Strava Auth Manager
# ==========================
class StravaAuthManager:
    def __init__(self):
        self.CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
        self.CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
        self.token_file = os.getenv("STRAVA_TOKEN_FILE", "strava_tokens.json")
        self.base_redirect_uri = "http://127.0.0.1"
        self.port = self.find_free_port()
        self.REDIRECT_URI = f"{self.base_redirect_uri}:{self.port}"

    def find_free_port(self):
        for port in range(8080, 8091):
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                try:
                    sock.bind(('127.0.0.1', port))
                    return port
                except OSError:
                    continue
        raise OSError("Geen vrije poort gevonden tussen 8080 en 8090")

    def load_tokens(self):
        if os.path.exists(self.token_file):
            with open(self.token_file, 'r') as f:
                return json.load(f)
        return None

    def save_tokens(self, tokens):
        os.makedirs(os.path.dirname(self.token_file) or ".", exist_ok=True)
        with open(self.token_file, 'w') as f:
            json.dump(tokens, f)

    def refresh_access_token(self, refresh_token):
        url = "https://www.strava.com/oauth/token"
        payload = {
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token"
        }
        response = requests.post(url, data=payload)
        response.raise_for_status()
        new_tokens = response.json()
        new_tokens['expires_at'] = datetime.now().timestamp() + new_tokens['expires_in']
        self.save_tokens(new_tokens)
        return new_tokens

    def get_valid_access_token(self):
        tokens = self.load_tokens()
        if not tokens:
            raise Exception("Geen tokens gevonden. Voer eerst handmatige authenticatie uit.")
        if datetime.now().timestamp() >= tokens['expires_at'] - 60:
            print("Access token verlopen, vernieuwen...")
            tokens = self.refresh_access_token(tokens['refresh_token'])
        return tokens['access_token']

# ==========================
# Strava Data Manager
# ==========================
class StravaDataManager:
    def __init__(self):
        self.auth = StravaAuthManager()
        self.access_token = self.auth.get_valid_access_token()
        self.csv_file = os.getenv("CSV_FILE", "activiteiten.csv")
        self.json_file = os.getenv("JSON_FILE", "activiteiten_raw.json")
        self.existing_details_file = os.getenv("DETAILS_FILE", "existing_details.json")
        self.gps_file = os.getenv("GPS_FILE", "strava_gps_data.csv")
        self.debug_file = os.getenv("DEBUG_FILE", "debug_log.txt")
        self.db_config = {
            'dbname': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.INITIAL_PAUSE = 2
        self.consecutive_rate_limits = 0
        os.makedirs(os.path.dirname(self.csv_file) or ".", exist_ok=True)

    def debug_log(self, message):
        with open(self.debug_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - {message}\n")
        print(f"DEBUG: {message}")

    # --------------------------
    # API helpers
    # --------------------------
    def handle_rate_limit(self, response=None):
        self.consecutive_rate_limits += 1
        pause = self.INITIAL_PAUSE
        if response and 'X-RateLimit-Usage' in response.headers:
            usage = int(response.headers['X-RateLimit-Usage'].split(',')[0])
            limit = int(response.headers['X-RateLimit-Limit'].split(',')[0])
            if usage >= limit:
                pause = 120
        self.debug_log(f"Rate limit bereikt, wachten {pause} seconden...")
        time.sleep(pause)
        return pause

    def load_existing_details(self):
        if os.path.exists(self.existing_details_file):
            with open(self.existing_details_file, 'r') as f:
                return {str(k): v for k,v in json.load(f).items()}
        return {}

    def save_existing_details(self, details):
        with open(self.existing_details_file, 'w') as f:
            json.dump(details, f)

    def get_existing_ids(self):
        existing_ids = set()
        if os.path.exists(self.csv_file):
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_ids.add(str(row.get('ID')))
        return existing_ids

    def get_activities(self, last_date=None):
        all_activities = []
        page = 1
        per_page = 200
        consecutive_empty = 0
        existing_ids = self.get_existing_ids()
        while consecutive_empty < 5:
            params = {"page": page, "per_page": per_page}
            if last_date:
                params['after'] = int(last_date.timestamp())
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get("https://www.strava.com/api/v3/athlete/activities",
                                    headers=headers, params=params)
            if response.status_code == 429:
                self.handle_rate_limit(response)
                continue
            response.raise_for_status()
            activities = response.json()
            if not activities:
                consecutive_empty += 1
                page += 1
                continue
            new_activities = [a for a in activities if str(a['id']) not in existing_ids]
            all_activities.extend(new_activities)
            page += 1
        self.debug_log(f"Totaal {len(all_activities)} nieuwe activiteiten gevonden")
        return all_activities

    def get_activity_details(self, activity_id, existing_details):
        str_id = str(activity_id)
        if str_id in existing_details:
            return existing_details[str_id]
        url = f"https://www.strava.com/api/v3/activities/{activity_id}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params = {"include_all_efforts": True}
        for _ in range(3):
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 429:
                self.handle_rate_limit(response)
                continue
            if response.status_code == 200:
                data = response.json()
                existing_details[str_id] = data
                self.save_existing_details(existing_details)
                time.sleep(self.INITIAL_PAUSE)
                return data
            response.raise_for_status()
        return None

    # --------------------------
    # CSV helpers
    # --------------------------
    def _prepare_activity_row(self, activity):
        gear = activity.get('gear', {}) if isinstance(activity.get('gear'), dict) else {}
        start_latlng = activity.get('start_latlng', [])
        end_latlng = activity.get('end_latlng', [])
        row = {
            'ID': activity.get('id'),
            'Naam': activity.get('name'),
            'Datum': activity.get('start_date'),
            'Type': activity.get('type'),
            'Afstand (km)': round(activity.get('distance',0)/1000,2),
            'Tijd (min)': round(activity.get('moving_time',0)/60,2),
            'Totale tijd (min)': round(activity.get('elapsed_time',0)/60,2),
            'Hoogtemeters': activity.get('total_elevation_gain'),
            'Gear ID': activity.get('gear_id'),
            'Gear naam': gear.get('name'),
            'Calorieën': activity.get('calories'),
            'Gemiddeld vermogen (W)': activity.get('average_watts'),
            'Max snelheid (km/u)': round(activity.get('max_speed',0)*3.6,2),
            'Start_lat': start_latlng[0] if len(start_latlng)>=1 else activity.get('start_latitude'),
            'Start_lng': start_latlng[1] if len(start_latlng)>=2 else activity.get('start_longitude'),
            'End_lat': end_latlng[0] if len(end_latlng)>=1 else activity.get('end_latitude'),
            'End_lng': end_latlng[1] if len(end_latlng)>=2 else activity.get('end_longitude'),
        }
        return row

    def save_activities_to_csv(self, activities):
        fieldnames = list(self._prepare_activity_row(activities[0]).keys())
        existing_ids = self.get_existing_ids()
        new_rows = [self._prepare_activity_row(a) for a in activities if str(a['id']) not in existing_ids]
        mode = 'a' if os.path.exists(self.csv_file) else 'w'
        with open(self.csv_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if mode=='w':
                writer.writeheader()
            writer.writerows(new_rows)
        self.debug_log(f"{len(new_rows)} activiteiten opgeslagen in CSV")

    # --------------------------
    # Main sync
    # --------------------------
    def sync_activities(self):
        self.debug_log("Start ophalen activiteiten...")
        last_date = None
        if os.path.exists(self.csv_file):
            with open(self.csv_file,'r') as f:
                reader = csv.DictReader(f)
                dates = [datetime.strptime(row['Datum'],"%Y-%m-%dT%H:%M:%SZ") for row in reader if row['Datum']]
                if dates: last_date = max(dates)
        summary_activities = self.get_activities(last_date)
        existing_details = self.load_existing_details()
        all_activities = []
        for act in summary_activities:
            detailed = self.get_activity_details(act['id'], existing_details) or act
            merged = {**act, **detailed}
            all_activities.append(merged)
        if all_activities:
            self.save_activities_to_csv(all_activities)
        self.debug_log("Sync klaar ✅")

# ==========================
# Run
# ==========================
if __name__ == "__main__":
    manager = StravaDataManager()
    manager.sync_activities()
