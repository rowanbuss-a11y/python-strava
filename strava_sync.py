import requests
import csv
import os
import json
import time
import socket
from datetime import datetime, timedelta
import polyline
import psycopg2
from psycopg2.extras import execute_values
from contextlib import closing

# --------------------------
# STRAVA AUTHENTICATIE
# --------------------------
class StravaAuthManager:
    CLIENT_ID = "129018"
    CLIENT_SECRET = "69d0ce2fdd3cdfc33b037b5e43d3f9f3faf0eed4"
    TOKEN_FILE = "/Users/rowanbuss/Desktop/STRAVA NIEUW/strava_tokens.json"
    
    def __init__(self):
        self.REDIRECT_URI = f"http://127.0.0.1:{self.find_free_port()}"
    
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
        if os.path.exists(self.TOKEN_FILE):
            with open(self.TOKEN_FILE, 'r') as f:
                return json.load(f)
        return None
    
    def save_tokens(self, tokens):
        os.makedirs(os.path.dirname(self.TOKEN_FILE), exist_ok=True)
        with open(self.TOKEN_FILE, 'w') as f:
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
        tokens = response.json()
        tokens['expires_at'] = datetime.now().timestamp() + tokens['expires_in']
        self.save_tokens(tokens)
        return tokens
    
    def get_valid_access_token(self):
        tokens = self.load_tokens()
        if not tokens:
            raise Exception("Geen tokens gevonden. Voer eerst eenmalig handmatige authenticatie uit.")
        if datetime.now().timestamp() >= tokens['expires_at'] - 60:
            print("Access token verlopen, vernieuwen...")
            tokens = self.refresh_access_token(tokens['refresh_token'])
        return tokens['access_token']

# --------------------------
# STRAVA DATA MANAGER
# --------------------------
class StravaDataManager:
    CSV_FILE = "/Users/rowanbuss/Desktop/STRAVA NIEUW/activiteiten.csv"
    JSON_FILE = "/Users/rowanbuss/Desktop/STRAVA NIEUW/activiteiten_raw.json"
    GPS_FILE = "/Users/rowanbuss/Desktop/STRAVA NIEUW/strava_gps_data.csv"
    DETAILS_FILE = "/Users/rowanbuss/Desktop/STRAVA NIEUW/existing_details.json"
    
    def __init__(self):
        self.auth = StravaAuthManager()
        self.access_token = self.auth.get_valid_access_token()
        os.makedirs(os.path.dirname(self.CSV_FILE), exist_ok=True)
        self.existing_details = self.load_existing_details()
    
    def debug_log(self, msg):
        print(f"[DEBUG {datetime.now().isoformat()}] {msg}")
    
    def load_existing_details(self):
        if os.path.exists(self.DETAILS_FILE):
            with open(self.DETAILS_FILE, 'r') as f:
                return {str(k): v for k, v in json.load(f).items()}
        return {}
    
    def save_existing_details(self):
        with open(self.DETAILS_FILE, 'w') as f:
            json.dump(self.existing_details, f)
    
    def get_existing_ids(self):
        ids = set()
        if os.path.exists(self.CSV_FILE):
            with open(self.CSV_FILE, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    ids.add(row['ID'])
        return ids
    
    def get_last_activity_date(self):
        if not os.path.exists(self.CSV_FILE):
            return None
        dates = []
        with open(self.CSV_FILE, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                try:
                    dates.append(datetime.strptime(row['Datum'], "%Y-%m-%dT%H:%M:%SZ"))
                except:
                    continue
        return max(dates) if dates else None
    
    def fetch_activities_summary(self, last_date=None):
        all_activities = []
        page = 1
        existing_ids = self.get_existing_ids()
        while True:
            params = {"page": page, "per_page": 200}
            if last_date:
                params['after'] = int(last_date.timestamp())
            resp = requests.get("https://www.strava.com/api/v3/athlete/activities",
                                headers={"Authorization": f"Bearer {self.access_token}"},
                                params=params)
            if resp.status_code != 200:
                break
            activities = resp.json()
            if not activities:
                break
            new_activities = [a for a in activities if str(a['id']) not in existing_ids]
            all_activities.extend(new_activities)
            page += 1
        self.debug_log(f"Fetched {len(all_activities)} new activities (summary)")
        return all_activities
    
    def fetch_activity_details(self, activity_id):
        if str(activity_id) in self.existing_details:
            return self.existing_details[str(activity_id)]
        url = f"https://www.strava.com/api/v3/activities/{activity_id}"
        resp = requests.get(url, headers={"Authorization": f"Bearer {self.access_token}"}, params={"include_all_efforts": True})
        if resp.status_code == 200:
            data = resp.json()
            self.existing_details[str(activity_id)] = data
            return data
        self.debug_log(f"Failed to fetch details for {activity_id}")
        return None
    
    def merge_summary_with_details(self, summaries):
        activities = []
        for s in summaries:
            details = self.fetch_activity_details(s['id'])
            if details:
                merged = {**s, **details}
            else:
                merged = s
            activities.append(merged)
        self.save_existing_details()
        return activities
    
    def save_to_csv(self, activities):
        if not activities:
            return
        fieldnames = list(self._prepare_row(activities[0]).keys())
        file_exists = os.path.exists(self.CSV_FILE)
        with open(self.CSV_FILE, 'a' if file_exists else 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for act in activities:
                writer.writerow(self._prepare_row(act))
        self.debug_log(f"Saved {len(activities)} activities to CSV")
    
    def save_gps_csv(self, activities):
        with open(self.GPS_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['ActivityID','ActivityName','Latitude','Longitude','Timestamp'])
            writer.writeheader()
            for act in activities:
                if 'map' in act and act['map'].get('summary_polyline'):
                    points = polyline.decode(act['map']['summary_polyline'])
                    start_time = datetime.strptime(act['start_date'], "%Y-%m-%dT%H:%M:%SZ")
                    for idx, (lat, lng) in enumerate(points):
                        writer.writerow({
                            'ActivityID': act['id'],
                            'ActivityName': act['name'],
                            'Latitude': lat,
                            'Longitude': lng,
                            'Timestamp': (start_time + timedelta(seconds=idx)).isoformat()
                        })
        self.debug_log(f"Saved GPS data to CSV")
    
    def _prepare_row(self, act):
        gear_name = act.get('gear', {}).get('name') if isinstance(act.get('gear'), dict) else None
        return {
            'ID': act.get('id'),
            'Naam': act.get('name'),
            'Datum': act.get('start_date'),
            'Type': act.get('type'),
            'Afstand (km)': round(act.get('distance', 0)/1000,2),
            'Tijd (min)': round(act.get('moving_time',0)/60,2),
            'Calorieën': act.get('calories'),
            'Gear naam': gear_name
        }
    
    def sync(self):
        last_date = self.get_last_activity_date()
        summaries = self.fetch_activities_summary(last_date)
        full_activities = self.merge_summary_with_details(summaries)
        self.save_to_csv(full_activities)
        self.save_gps_csv(full_activities)
        self.debug_log("Sync voltooid ✅")

# --------------------------
# RUN SYNC
# --------------------------
if __name__ == "__main__":
    manager = StravaDataManager()
    manager.sync()
