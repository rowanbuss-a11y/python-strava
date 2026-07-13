# Garmin sync — gratis, alleen jouw eigen account

Haalt jouw Garmin-activiteiten automatisch op en zet ze naast je Strava-data in
Nexalyze, met een bron-schakelaar in het dashboard. Kosten: **€0**.

```
garmin_setup.py (1× lokaal)  →  GARMIN_TOKENS secret
GitHub Actions (elk uur)     →  garmin_sync.py
   • login via token (geen wachtwoord opgeslagen)
   • haalt jouw activiteiten op
   • schrijft naar strava_activities  (source='garmin', user_id = jij)
Nexalyze dashboard           →  schakelaar Strava / Garmin / Alles (met dedup)
```

> Werkt alleen voor **je eigen account**. Multi-user Garmin vereist de officiële
> Garmin API (rechtspersoon) of een betaalde aggregator — zie de gespreksnotities.

---

## 1. Database migreren

Voer de migratie uit die de `source`-kolom toevoegt:

```bash
cd strava-dashboard-main
supabase db push --project-ref lqpdxitcqnfbsikdbopq
```
(Al toegepast op lqpd op 2026-07-02 — de `source`-kolom bestaat al.)

(Of plak `supabase/migrations/20260610120000_add_activity_source.sql` in de SQL-editor.)

## 2. Garmin-token genereren (eenmalig, lokaal)

```bash
pip install garminconnect
python garmin_setup.py
```

Log één keer in (voer je MFA-code in als je tweestapsverificatie aan hebt).
Het script print een base64-string — die zet je zo als GitHub-secret.

## 3. GitHub Secrets instellen

In je Python-sync-repo → **Settings → Secrets and variables → Actions**:

| Secret           | Waarde                                                        |
|------------------|--------------------------------------------------------------|
| `GARMIN_TOKENS`  | De base64-string uit stap 2                                  |
| `OWNER_USER_ID`  | Jouw Supabase auth user-id (Dashboard → Authentication → jouw user → UID) |
| `SUPABASE_URL`   | `https://lqpdxitcqnfbsikdbopq.supabase.co` (project `strava_activities`) |
| `SUPABASE_KEY`   | Service-role key van **lqpd** (Dashboard → Settings → API → service_role) |

## 4. Bestanden pushen

Push `garmin_sync.py`, `garmin_setup.py`, `requirements`-deps en
`.github/workflows/garmin-sync.yml` naar je sync-repo.

## 5. Testen

GitHub → **Actions → Garmin sync → Run workflow** (input `manual`).
Check de logs: "X Garmin-activiteiten geupload". Daarna draait hij elk uur.

---

## Onderhoud & aandachtspunten

- **Token verloopt na ~1 jaar** → draai `garmin_setup.py` opnieuw en vervang `GARMIN_TOKENS`.
- **Dubbele activiteiten**: als je Garmin óók automatisch naar Strava pusht, staat
  elke training 2× (Strava + Garmin). Het dashboard ontdubbelt deze in de
  "Alles"-weergave (zelfde starttijd + duur → Strava wint). Per bron zie je alles.
- **Kosten**: GitHub Actions is gratis (publieke repo onbeperkt; privé 2000 min/mnd,
  een run duurt ~1-2 min).
