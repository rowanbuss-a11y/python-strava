import os
import requests
import json
import csv
import time
import polyline
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

# =========================================
# Strava Authentication Manager
# =========================================
class StravaAuthManager:
    def __init__(self):
        self.CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
        self.CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
        self.REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
        self.token_file = os.getenv("STRAVA_TOKEN_FILE", "strava_tokens.json")
        self.access_token = None

    def get_valid_access_token(self):
        # Gebruik token in memory als die bestaat
        if self.access_token:
            return self.access_token

        # Ververs token via refresh token
        if self.REFRESH_TOKEN:
            url = "https://www.strava.com/oauth/token"
            payload = {
                "client_id": self.CLIENT_ID,
                "client_secret": self.CLIENT_SECRET,
                "refresh_token": self.REFRESH_TOKEN,
                "grant_type": "refresh_token"
            }
            response = requests.post(url, data=payload)
            response.raise_for_status()
            tokens = response.json()
            self.access_token = tokens['access_token']
            # update REFRESH_TOKEN voor volgende run
            self.REFRESH_TOKEN = tokens.get('refresh_token', self.REFRESH_TOKEN)
            return self.access_token

        # fallback op lokaal tokenbestand
        if os.path.exists(self.token_file):
            with open(self.token_file, 'r') as f:
                tokens = json.load(f)
            self.access_token = tokens['access_token']
            return self.access_token

        raise Exception("Geen tokens gevonden. Zet STRAVA_REFRESH_TOKEN als secret in GitHub Actions")


# =========================================
# Strava Data Manager
# =========================================
class StravaDataManager:
    def __init__(self):
        self.auth = StravaAuthManager()
        self.access_token = self.auth.get_valid_access_token()
        self.csv_file = os.getenv("CSV_FILE", "activiteiten.csv")
        self.json_file = os.getenv("JSON_FILE", "activiteiten_raw.json")
        self.debug_file = os.getenv("DEBUG_FILE", "debug_log.txt")
        self.gps_file = os.getenv("GPS_FILE", "strava_gps_data.csv")
        self.existing_details_file = os.getenv("EXISTING_DETAILS_FILE", "existing_details.json")

        # Database config (Supabase/PostgreSQL)
        self.db_config = {
            'dbname': os.getenv("DB_NAME", "postgres"),
            'user': os.getenv("DB_USER", "postgres"),
            'password': os.getenv("DB_PASSWORD"),
            'host': os.getenv("DB_HOST"),
            'port': os.getenv("DB_PORT", 5432)
        }

        self.debug_log("StravaDataManager initialized.")

    # =========================================
    # Debug log
    # =========================================
    def debug_log(self, message):
        timestamp = datetime.now().isoformat()
        with open(self.debug_file, 'a', encoding='utf-8') as f:
            f.write(f"{timestamp} - {message}\n")
        print(f"DEBUG: {message}")

    # =========================================
    # Haal bestaande details op
    # =========================================
    def load_existing_details(self):
        if os.path.exists(self.existing_details_file):
            try:
                with open(self.existing_details_file, 'r') as f:
                    data = json.load(f)
                    return {str(k): v for k, v in data.items()}
            except:
                return {}
        return {}

    def save_existing_details(self, details_dict):
        with open(self.existing_details_file, 'w') as f:
            json.dump(details_dict, f)

    # =========================================
    # Haal activiteiten op van Strava
    # =========================================
    def get_activities(self, page=1, per_page=200):
        all_activities = []
        headers = {"Authorization": f"Bearer {self.access_token}"}

        while True:
            url = "https://www.strava.com/api/v3/athlete/activities"
            params = {"page": page, "per_page": per_page}
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                self.debug_log(f"Fout bij ophalen activiteiten: {response.text}")
                break
            activities = response.json()
            if not activities:
                break
            all_activities.extend(activities)
            page += 1

        self.debug_log(f"Totaal {len(all_activities)} activiteiten opgehaald.")
        return all_activities

    # =========================================
    # Haal gedetailleerde data op per activiteit
    # =========================================
    def get_activity_details(self, activity_id):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = f"https://www.strava.com/api/v3/activities/{activity_id}"
        params = {"include_all_efforts": True}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            self.debug_log(f"Fout bij ophalen details {activity_id}: {response.text}")
            return None

    # =========================================
    # Voorbereiden CSV rij
    # =========================================
    def _prepare_activity_row(self, activity):
        gear = activity.get('gear', {}) if isinstance(activity.get('gear'), dict) else {}
        row = {
            "ID": activity.get('id'),
            "Naam": activity.get('name'),
            "Datum": activity.get('start_date'),
            "Type": activity.get('type'),
            "Afstand (km)": round(activity.get('distance', 0)/1000,2),
            "Tijd (min)": round(activity.get('moving_time',0)/60,2),
            "Totale tijd (min)": round(activity.get('elapsed_time',0)/60,2),
            "Hoogtemeters": activity.get('total_elevation_gain',0),
            "Calorieën": activity.get('calories'),
            "Gear naam": gear.get('name'),
            "Start_lat": activity.get('start_latitude'),
            "Start_lng": activity.get('start_longitude')
        }
        return row

    # =========================================
    # Opslaan in CSV
    # =========================================
    def save_activities_to_csv(self, activities):
        fieldnames = list(self._prepare_activity_row(activities[0]).keys())
        file_exists = os.path.exists(self.csv_file)
        with open(self.csv_file, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for act in activities:
                writer.writerow(self._prepare_activity_row(act))
        self.debug_log(f"{len(activities)} activiteiten opgeslagen in CSV.")

    # =========================================
    # Opslaan GPS data
    # =========================================
    def save_gps_data(self, activities):
        with open(self.gps_file, mode='w', newline='', encoding='utf-8') as f:
            fieldnames = ['ActivityID','Latitude','Longitude']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for act in activities:
                map_data = act.get('map', {})
                if 'summary_polyline' in map_data:
                    points = polyline.decode(map_data['summary_polyline'])
                    for lat,lng in points:
                        writer.writerow({
                            'ActivityID': act.get('id'),
                            'Latitude': lat,
                            'Longitude': lng
                        })
        self.debug_log("GPS data opgeslagen.")

    # =========================================
    # Opslaan in Supabase/Postgres
    # =========================================
    def save_to_database(self, activities):
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    for act in activities:
                        cur.execute("""
                            INSERT INTO strava_activities (id, name, type, start_date, distance, moving_time, calories, gear_name)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (id) DO UPDATE SET
                            name=EXCLUDED.name, type=EXCLUDED.type, distance=EXCLUDED.distance,
                            moving_time=EXCLUDED.moving_time, calories=EXCLUDED.calories, gear_name=EXCLUDED.gear_name
                        """, (
                            act.get('id'), act.get('name'), act.get('type'), act.get('start_date'),
                            act.get('distance'), act.get('moving_time'), act.get('calories'),
                            (act.get('gear', {}) or {}).get('name')
                        ))
                    conn.commit()
            self.debug_log(f"{len(activities)} activiteiten opgeslagen in database.")
        except Exception as e:
            self.debug_log(f"Fout bij opslaan in database: {e}")

    # =========================================
    # Main functie
    # =========================================
    def run(self):
        self.debug_log("Start ophalen Strava activiteiten...")
        summary_activities = self.get_activities()
        detailed_activities = []

        for act in summary_activities:
            detail = self.get_activity_details(act['id'])
            if detail:
                detailed_activities.append(detail)
            else:
                detailed_activities.append(act)

        self.save_activities_to_csv(detailed_activities)
        self.save_gps_data(detailed_activities)
        self.save_to_database(detailed_activities)
        self.debug_log("Sync afgerond ✅")


# =========================================
# Start script
# =========================================
if __name__ == "__main__":
    manager = StravaDataManager()
    manager.run()
