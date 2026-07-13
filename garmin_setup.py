#!/usr/bin/env python3
"""
garmin_setup.py — Eenmalige Garmin Connect authenticatie en token opslag.

Gebruik:
    pip install garminconnect
    python garmin_setup.py

De tokens worden opgeslagen in ~/.garmin_tokens.json.
Kopieer de base64-encoded inhoud naar je GitHub Secret GARMIN_TOKENS.
"""

import base64
import json
import os
import sys
from pathlib import Path

try:
    from garminconnect import Garmin
except ImportError:
    print("❌ garminconnect is niet geïnstalleerd.")
    print("   Voer uit: pip install garminconnect")
    sys.exit(1)

TOKEN_FILE = Path.home() / ".garmin_tokens.json"


def main():
    email = input("Garmin e-mailadres: ").strip()
    password = input("Garmin wachtwoord: ").strip()

    print("\n⏳ Inloggen bij Garmin Connect...")
    try:
        client = Garmin(email, password)
        client.login()
    except Exception as e:
        print(f"❌ Inloggen mislukt: {e}")
        sys.exit(1)

    # Sla tokens op
    tokens = client.garth.dumps()
    TOKEN_FILE.write_text(tokens)
    print(f"✅ Tokens opgeslagen in {TOKEN_FILE}")

    # Base64 voor GitHub Secret
    encoded = base64.b64encode(tokens.encode()).decode()
    print("\n── GitHub Secret ───────────────────────────────────────")
    print("Naam:   GARMIN_TOKENS")
    print("Waarde (kopieer alles hieronder):")
    print(encoded)
    print("─────────────────────────────────────────────────────────")
    print("\nVoeg dit toe als GitHub Secret op:")
    print("  https://github.com/<OWNER>/<REPO>/settings/secrets/actions/new")


if __name__ == "__main__":
    main()
