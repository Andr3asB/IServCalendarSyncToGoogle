from __future__ import annotations
import os, json, base64, hashlib, logging, sys
from datetime import datetime, timedelta, timezone

from caldav import DAVClient
from icalendar import Calendar
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# ---------- Konfiguration aus ENV ----------
CALDAV_URL   = os.environ.get("CALDAV_URL", "").strip()
CALDAV_USER  = os.environ.get("CALDAV_USER", "").strip()
CALDAV_PASS  = os.environ.get("CALDAV_PASS", "").strip()
GOOGLE_CAL_ID = os.environ.get("GOOGLE_CAL_ID", "").strip()

# Zeitraum: wie weit in die Zukunft/ Vergangenheit schauen
DAYS_AHEAD   = int(os.environ.get("DAYS_AHEAD", "14"))
DAYS_PAST    = int(os.environ.get("DAYS_PAST", "1"))

# Google-Creds: entweder Pfad oder Base64
GOOGLE_CREDENTIALS_FILE   = os.environ.get("GOOGLE_CREDENTIALS_FILE", "").strip()
GOOGLE_CREDENTIALS_JSON_B64 = os.environ.get("GOOGLE_CREDENTIALS_JSON_B64", "").strip()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# ---------- Logging ----------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("iserv-sync")

def load_google_credentials():
    # 1) Base64-json (z. B. aus Docker Secret)
    if GOOGLE_CREDENTIALS_JSON_B64:
        try:
            data = base64.b64decode(GOOGLE_CREDENTIALS_JSON_B64)
            creds_info = json.loads(data.decode("utf-8"))
            return Credentials.from_service_account_info(
                creds_info,
                scopes=["https://www.googleapis.com/auth/calendar"]
            )
        except Exception as e:
            log.error("Konnte GOOGLE_CREDENTIALS_JSON_B64 nicht laden: %s", e)
            sys.exit(2)
    # 2) Datei-Pfad (gemountete Secret-Datei)
    if GOOGLE_CREDENTIALS_FILE:
        try:
            return Credentials.from_service_account_file(
                GOOGLE_CREDENTIALS_FILE,
                scopes=["https://www.googleapis.com/auth/calendar"]
            )
        except Exception as e:
            log.error("Konnte GOOGLE_CREDENTIALS_FILE nicht laden: %s", e)
            sys.exit(2)
    log.error("Keine Google-Credentials gesetzt (GOOGLE_CREDENTIALS_JSON_B64 oder GOOGLE_CREDENTIALS_FILE).")
    sys.exit(2)

def to_rfc3339(dt):
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    return None  # date-only separat behandeln

def parse_vevents(ics_bytes: bytes):
    cal = Calendar.from_ical(ics_bytes)
    for comp in cal.walk("VEVENT"):
        uid = str(comp.get("UID", "")).strip()
        summary = str(comp.get("SUMMARY","")).strip()
        description = str(comp.get("DESCRIPTION","")).strip()
        dtstart = comp.decoded("DTSTART")
        dtend   = comp.decoded("DTEND", fallback=None)
        lastmod = comp.get("LAST-MODIFIED") or comp.get("DTSTAMP")

        if not uid:
            raw = (summary + str(dtstart) + str(dtend)).encode()
            uid = "fallback-" + hashlib.sha1(raw).hexdigest()

        if lastmod:
            lm = lastmod.dt
            if isinstance(lm, datetime):
                lastmod_iso = lm.astimezone(timezone.utc).isoformat()
            else:
                lastmod_iso = datetime(lm.year, lm.month, lm.day, tzinfo=timezone.utc).isoformat()
        else:
            lastmod_iso = None

        # payloads für Google
        if isinstance(dtstart, datetime):
            start_payload = {"dateTime": to_rfc3339(dtstart)}
        else:
            start_payload = {"date": dtstart.isoformat()}

        if dtend:
            if isinstance(dtend, datetime):
                end_payload = {"dateTime": to_rfc3339(dtend)}
            else:
                end_payload = {"date": dtend.isoformat()}
        else:
            if isinstance(dtstart, datetime):
                end_payload = {"dateTime": to_rfc3339(dtstart + timedelta(hours=1))}
            else:
                end_payload = {"date": dtstart.isoformat()}

        yield {
            "uid": uid,
            "summary": summary,
            "description": description,
            "start": start_payload,
            "end": end_payload,
            "lastmod": lastmod_iso,
        }

def ensure_event(service, calendar_id: str, event, time_min_iso: str, time_max_iso: str):
    # Suche nach existierendem Event via privateExtendedProperty
    filters = [f"source=ISERV", f"uid={event['uid']}"]
    resp = service.events().list(
        calendarId=calendar_id,
        timeMin=time_min_iso,
        timeMax=time_max_iso,
        singleEvents=True,
        privateExtendedProperty=filters,
        maxResults=2,
    ).execute()
    items = resp.get("items", [])

    if not items:
        body = {
            "summary": event["summary"],
            "description": (event["description"] or "") + "\n\n[SYNC: ISERV]",
            "start": event["start"],
            "end": event["end"],
            "extendedProperties": {
                "private": {
                    "source": "ISERV",
                    "uid": event["uid"],
                    "lastmod": event["lastmod"] or "",
                }
            }
        }
        created = service.events().insert(calendarId=calendar_id, body=body).execute()
        log.info("Created: %s  %s", created.get("id"), event["summary"])
        return

    g_event = items[0]
    g_priv = (g_event.get("extendedProperties") or {}).get("private", {})
    if (event["lastmod"] or "") != g_priv.get("lastmod", ""):
        patch = {
            "summary": event["summary"],
            "description": (event["description"] or "") + "\n\n[SYNC: ISERV]",
            "start": event["start"],
            "end": event["end"],
            "extendedProperties": {"private": {**g_priv, "lastmod": event["lastmod"] or ""}},
        }
        updated = service.events().patch(calendarId=calendar_id, eventId=g_event["id"], body=patch).execute()
        log.info("Updated: %s  %s", updated.get("id"), event["summary"])
    else:
        log.debug("No change: %s  %s", g_event.get("id"), event["summary"])

def main():
    # Sanity Checks
    missing = [k for k,v in {
        "CALDAV_URL": CALDAV_URL,
        "CALDAV_USER": CALDAV_USER,
        "CALDAV_PASS": CALDAV_PASS,
        "GOOGLE_CAL_ID": GOOGLE_CAL_ID,
    }.items() if not v]
    if missing:
        log.error("Fehlende ENV Variablen: %s", ", ".join(missing))
        sys.exit(2)

    creds = load_google_credentials()
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=DAYS_PAST)
    end   = now + timedelta(days=DAYS_AHEAD)
    time_min_iso = start.isoformat()
    time_max_iso = end.isoformat()

    # CalDAV abrufen
    client = DAVClient(url=CALDAV_URL, username=CALDAV_USER, password=CALDAV_PASS)
    principal = client.principal()
    calendars = principal.calendars()
    if not calendars:
        log.error("Kein CalDAV-Kalender gefunden (URL korrekt? Berechtigungen?)")
        sys.exit(3)
    calendar = calendars[0]

    # Events als ICS im Zeitraum ziehen
    ical_objs = calendar.date_search(start=start, end=end)
    total = 0
    for obj in ical_objs:
        for ev in parse_vevents(obj.data):
            ensure_event(service, GOOGLE_CAL_ID, ev, time_min_iso, time_max_iso)
            total += 1

    log.info("Fertig. Verarbeitete Events: %d (Zeitraum %s → %s)", total, time_min_iso, time_max_iso)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Unerwarteter Fehler: %s", e)
        sys.exit(1)
