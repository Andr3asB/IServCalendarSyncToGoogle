from __future__ import annotations
import hashlib
from datetime import datetime, timezone, timedelta

from caldav import DAVClient
from icalendar import Calendar
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# --- Konfiguration ---
CALDAV_URL      = "https://iserv.example.org/caldav/kalender"  # deine CalDAV-URL
CALDAV_USER     = "user"
CALDAV_PASS     = "pass"
GOOGLE_CAL_ID   = "dein_google_kalender_id@group.calendar.google.com"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# --- Google Service aufbauen (Service Account-Beispiel) ---
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
service = build("calendar", "v3", credentials=creds)

# --- CalDAV abrufen (als ICS) ---
client = DAVClient(url=CALDAV_URL, username=CALDAV_USER, password=CALDAV_PASS)
principal = client.principal()
calendars = principal.calendars()
if not calendars:
    raise RuntimeError("Kein CalDAV-Kalender gefunden.")
calendar = calendars[0]

# Zeitraum definieren (z. B. nächste 14 Tage)
now = datetime.now(timezone.utc)
time_min = now.isoformat()
time_max = (now + timedelta(days=14)).isoformat()
ical_objs = calendar.date_search(start=now, end=now + timedelta(days=14))

def parse_vevents(ics_bytes: bytes):
    cal = Calendar.from_ical(ics_bytes)
    for comp in cal.walk("VEVENT"):
        uid = str(comp.get("UID", "")).strip()
        if not uid:  # ohne UID überspringen (oder eigenen Hash bilden)
            # Fallback-Key (nicht ideal, aber möglich)
            raw = (str(comp.get("SUMMARY","")) + str(comp.get("DTSTART","")) + str(comp.get("DTEND",""))).encode()
            uid = "fallback-" + hashlib.sha1(raw).hexdigest()
        summary = str(comp.get("SUMMARY","")).strip()
        description = str(comp.get("DESCRIPTION","")).strip()
        dtstart = comp.decoded("DTSTART")
        dtend   = comp.decoded("DTEND", fallback=None)
        lastmod = comp.get("LAST-MODIFIED") or comp.get("DTSTAMP")
        lastmod_iso = str(lastmod.dt.astimezone(timezone.utc).isoformat()) if lastmod else None

        # Zeiten in RFC3339 umwandeln (ganztägig beachten)
        def to_rfc3339(dt):
            if isinstance(dt, datetime):
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat()
            else:  # date => ganztägig
                return {"date": dt.isoformat()}

        start_payload = {"dateTime": to_rfc3339(dtstart)} if isinstance(dtstart, datetime) else {"date": dtstart.isoformat()}
        end_payload   = {"dateTime": to_rfc3339(dtend)} if (dtend and isinstance(dtend, datetime)) else ({"date": dtend.isoformat()} if dtend else None)
        if end_payload is None:
            # Falls kein DTEND gesetzt ist, einfache Heuristik: 1h Termin
            if isinstance(dtstart, datetime):
                end_payload = {"dateTime": (dtstart + timedelta(hours=1)).astimezone(timezone.utc).isoformat()}
            else:
                end_payload = {"date": dtstart.isoformat()}  # ganztägig

        yield {
            "uid": uid,
            "summary": summary,
            "description": description,
            "start": start_payload,
            "end": end_payload,
            "lastmod": lastmod_iso,
        }

def ensure_event(e):
    # 1) Existenzprüfung per extendedProperties.private
    #    -> filter über 'privateExtendedProperty' (AND, wenn mehrfach angegeben)
    filters = [f"source=ISERV", f"uid={e['uid']}"]
    existing = service.events().list(
        calendarId=GOOGLE_CAL_ID,
        timeMin=time_min, timeMax=time_max,
        singleEvents=True,
        privateExtendedProperty=filters
    ).execute()
    items = existing.get("items", [])

    if not items:
        # 2) Neu anlegen
        body = {
            "summary": e["summary"],
            "description": (e["description"] or "") + "\n\n[SYNC: ISERV]",
            "start": e["start"],
            "end": e["end"],
            "extendedProperties": {
                "private": {
                    "source": "ISERV",
                    "uid": e["uid"],
                    "lastmod": e["lastmod"] or "",
                }
            }
        }
        created = service.events().insert(calendarId=GOOGLE_CAL_ID, body=body).execute()
        print("Created:", created.get("id"), e["summary"])
        return

    # 3) Update nur wenn sich LAST-MODIFIED geändert hat
    g_event = items[0]
    g_priv = (g_event.get("extendedProperties") or {}).get("private", {})
    if (e["lastmod"] or "") != g_priv.get("lastmod", ""):
        patch = {
            "summary": e["summary"],
            "description": (e["description"] or "") + "\n\n[SYNC: ISERV]",
            "start": e["start"],
            "end": e["end"],
            "extendedProperties": {"private": {**g_priv, "lastmod": e["lastmod"] or ""}},
        }
        updated = service.events().patch(calendarId=GOOGLE_CAL_ID, eventId=g_event["id"], body=patch).execute()
        print("Updated:", updated.get("id"), e["summary"])
    else:
        print("No change:", g_event.get("id"), e["summary"])

for obj in ical_objs:
    for ev in parse_vevents(obj.data):
        ensure_event(ev)
