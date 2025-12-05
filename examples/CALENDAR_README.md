# Kalender-Management-System

## Übersicht

Importiert iCal-Kalender in eine hierarchische Struktur:
- **ObjectStore**: Speichert jeden Termin als JSON-Objekt
- **GraphLayer**: Verwaltet hierarchische Beziehungen

## Hierarchie-Struktur

```
Jahr (2026)
  ├─ Monat (Januar, Februar, ...)
  │   └─ Tag (2026-01-15)
  │       └─ Event (Meeting, Vorlesung, ...)
  │
  └─ Woche (2026-W03)
      └─ Tag (2026-01-15)
          └─ Event (...)
```

## Schemas

### Year
- **Attribute**: year, event_count

### Month  
- **Attribute**: year, month, month_name, event_count

### Week
- **Attribute**: year, week, start_date, end_date, event_count

### Day
- **Attribute**: date, year, month, day, weekday, event_count

### Event
- **Attribute**: date, start_time, end_time, duration_minutes, summary, status, object_store_id, object_url

## Installation

```bash
# Abhängigkeiten installieren
pip install icalendar requests Pillow --break-system-packages
```

## Verwendung

### 1. Server starten

**Terminal 1: ObjectStore**
```bash
python -m objectstore.cli.main server --port 5000
```

**Terminal 2: GraphLayer**
```bash
cd graphlayer
python graph_api.py --port 5001
```

### 2. Kalender importieren

**Terminal 3: Import & Query**
```bash
python calendar_manager.py \
  --ical-file /path/to/calendar.ics \
  --objectstore http://localhost:5000 \
  --graphlayer http://localhost:5001
```

### 3. Interaktive Abfragen

Das Programm bietet folgende Abfragemöglichkeiten:

1. **Bestimmter Tag**: `2026-01-15`
2. **Bestimmte Woche**: `2026-03` (Jahr-Woche)
3. **Bestimmter Monat**: `2026-01` (Jahr-Monat)
4. **Bestimmtes Jahr**: `2026`
5. **Alle Termine**

## Beispiel-Session

```
Welche Termine möchten Sie anzeigen?
================================================================================
  1) Bestimmter Tag (YYYY-MM-DD)
  2) Bestimmte Woche (YYYY-WW)
  3) Bestimmter Monat (YYYY-MM)
  4) Bestimmtes Jahr (YYYY)
  5) Alle Termine
  0) Beenden
================================================================================

Ihre Wahl: 1
Datum (YYYY-MM-DD): 2026-02-23

Termine am 2026-02-23
------------------------------------------------------------

📅 Montag, 23.02.2026
   (1 Termin)
------------------------------------------------------------
  ⏰ 08:00 - 15:15 (435 min)
     📝 Sichere Produktentwicklung
     🔗 http://localhost:5000/api/v1/objects/node01-abc123...
```

## Nur Import (ohne Abfrage)

```bash
python calendar_manager.py \
  --ical-file calendar.ics \
  --import-only
```

## Features

✓ Hierarchische Datums-Struktur (Jahr → Monat → Woche → Tag → Event)
✓ Jeder Termin als JSON im ObjectStore
✓ Vollständige Metadaten (Datum, Zeit, Dauer, Beschreibung)
✓ Kalenderwochen-Unterstützung
✓ Deutsche Wochentage und Monatsnamen
✓ Sortierte Ausgabe nach Datum und Uhrzeit
✓ Interaktive Abfragefunktion
✓ Klickbare Links zu ObjectStore-Objekten

## Datenstruktur

Jeder Event wird als JSON im ObjectStore gespeichert:

```json
{
  "uid": "78jgotf17haoe70jj6q8ev80e2@google.com",
  "summary": "IoT Embedded Systeme",
  "description": "",
  "status": "CONFIRMED",
  "start": "2026-02-20T08:00:00+00:00",
  "end": "2026-02-20T11:15:00+00:00",
  "duration_minutes": 195,
  "created": "2025-11-05T14:09:10+00:00",
  "last_modified": "2025-11-05T14:09:10+00:00"
}
```

## Tipps

- Termine werden automatisch nach Startzeit sortiert
- Jeder Tag zeigt die Anzahl der Termine
- ObjectStore-URLs sind direkt abrufbar
- Die Hierarchie ermöglicht effiziente Abfragen
- Kalenderwochen folgen ISO 8601 (Montag als erster Tag)
