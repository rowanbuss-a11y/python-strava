#!/usr/bin/env python3
"""
garmin_setup.py — Eenmalige Garmin Connect authenticatie en token opslag.

Gebruik:
    python3 -m pip install garminconnect
    python3 garmin_setup.py

Vraagt om je e-mail, wachtwoord (verborgen) en — als tweestapsverificatie
aanstaat — je MFA-code. De tokens komen in ~/.garmin_tokens.json; de base64-
string die geprint wordt zet je in het GitHub Secret GARMIN_TOKENS.
"""

import base64
import getpass
import sys
from pathlib import Path

try:
    from garminconnect import Garmin
except ImportError:
    print("❌ garminconnect is niet geïnstalleerd.")
    print("   Voer uit: python3 -m pip install garminconnect")
    sys.exit(1)

TOKEN_FILE = Path.home() / ".garmin_tokens.json"


def prompt_mfa():
    """Wordt door garminconnect aangeroepen als MFA nodig is."""
    return input("🔐 Garmin MFA-code (uit je Authenticator-app of sms): ").strip()


def build_client(email, password):
    """Maakt een Garmin-client; ondersteunt zowel nieuwe als oude garminconnect."""
    try:
        return Garmin(email=email, password=password, prompt_mfa=prompt_mfa)
    except TypeError:
        # Oudere garminconnect zonder prompt_mfa-parameter
        return Garmin(email, password)


def main():
    email = input("Garmin e-mailadres: ").strip()
    password = getpass.getpass("Garmin wachtwoord (wordt niet getoond): ").strip()

    print("\n⏳ Inloggen bij Garmin Connect...")
    try:
        client = build_client(email, password)
        client.login()
    except Exception as e:
        msg = str(e)
        print(f"❌ Inloggen mislukt: {msg}")
        if "429" in msg or "rate" in msg.lower():
            print("\n   → Garmin heeft je IP tijdelijk geblokkeerd (te veel pogingen).")
            print("     Wacht 30–60 minuten. Log ondertussen één keer in op")
            print("     https://connect.garmin.com in je browser om te bevestigen dat")
            print("     je account en wachtwoord werken, en probeer daarna opnieuw.")
        elif "mfa" in msg.lower():
            print("\n   → MFA-code werd niet geaccepteerd of niet gevraagd. Zorg dat je")
            print("     tweestapsverificatie-app of sms bij de hand is en probeer opnieuw.")
        sys.exit(1)

    tokens = client.garth.dumps()
    TOKEN_FILE.write_text(tokens)
    print(f"\n✅ Tokens opgeslagen in {TOKEN_FILE}")

    encoded = base64.b64encode(tokens.encode()).decode()
    print("\n── GARMIN_TOKENS (kopieer de hele regel hieronder) ──────")
    print(encoded)
    print("─────────────────────────────────────────────────────────")
    print("\nPlak deze string in de chat, of zet 'm als GitHub Secret 'GARMIN_TOKENS'.")


if __name__ == "__main__":
    main()
