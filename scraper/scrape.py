"""
TourMap — Full Self-Sustaining Scraper
=======================================
Scrapes every viable tour for:
  - Registration deadlines & opening dates
  - Course / location / date changes
  - Past results / leaderboards
  - NEW tournament discovery -> new_tournaments_pending.json for review

Coverage:
  ✅ GolfGenius / TYGA / CGA  — Playwright (JS-rendered)
  ✅ HJGT                     — Playwright (JS-rendered)
  ✅ AJGA                     — Playwright (JS-rendered)
  ✅ DGT                      — httpx (fully server-rendered, no JS needed)
  ✅ USKids Golf               — httpx (server-rendered)
  ✅ Pinehurst                 — Playwright
  ❌ USGA                     — registration portal only, no per-event data
  ❌ PGA Jr.                  — single event, manual is fine

Outputs:
  tournament_data.json          — patches existing tournament records
  new_tournaments_pending.json  — newly discovered events awaiting review
"""

import asyncio
import json
import re
import sys
from datetime import datetime, timezone, date
from pathlib import Path

import httpx
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT           = Path(__file__).parent.parent
OUTPUT_PATH    = ROOT / "tournament_data.json"
PENDING_PATH   = ROOT / "new_tournaments_pending.json"
TOUR_LIST_PATH = Path(__file__).parent / "tournament_list.py"

TODAY     = date.today().isoformat()
THIS_YEAR = date.today().year
TIMEOUT   = 30_000  # ms (Playwright)
DELAY     = 1.2     # seconds between requests

# ── Carolinas filter for discovery ───────────────────────────────────────────
CAROLINAS_RE = re.compile(
    r"\b(NC|SC|North Carolina|South Carolina|"
    r"Raleigh|Charlotte|Greensboro|Durham|Winston.Salem|Asheville|"
    r"Wilmington|Fayetteville|Cary|Pinehurst|Southern Pines|"
    r"Columbia|Charleston|Greenville|Myrtle Beach|Hilton Head|"
    r"Florence|Rock Hill|Spartanburg|Pawleys Island|"
    r"High Point|Concord|Gastonia|Mooresville|Burlington|"
    r"Snow Hill|Goldsboro|New Bern|Sanford|Salisbury|"
    r"Weddington|Locust|Wallace|Lake Wylie)\b",
    re.I,
)

def is_carolinas(text: str) -> bool:
    return bool(CAROLINAS_RE.search(text or ""))

# ── Date helpers ──────────────────────────────────────────────────────────────
MONTH_ABBR = {
    "jan": "Jan", "january": "Jan",
    "feb": "Feb", "february": "Feb",
    "mar": "Mar", "march": "Mar",
    "apr": "Apr", "april": "Apr",
    "may": "May",
    "jun": "Jun", "june": "Jun",
    "jul": "Jul", "july": "Jul",
    "aug": "Aug", "august": "Aug",
    "sep": "Sep", "september": "Sep",
    "oct": "Oct", "october": "Oct",
    "nov": "Nov", "november": "Nov",
    "dec": "Dec", "december": "Dec",
}
MONTH_NUM = {v: i+1 for i, v in enumerate(
    ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
)}
DATE_RE = re.compile(
    r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
    r"Dec(?:ember)?)\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s*(\d{4}))?",
    re.I,
)

def fmt_date(raw: str, year: int | None = None) -> str | None:
    m = DATE_RE.search(raw or "")
    if not m:
        return None
    mon = MONTH_ABBR.get(m.group(1).lower().rstrip("."))
    if not mon:
        return None
    day = int(m.group(2))
    yr  = int(m.group(3)) if m.group(3) else (year or THIS_YEAR)
    try:
        dt = datetime(yr, MONTH_NUM[mon], day)
        return f"{mon} {dt.day}, {yr}"
    except ValueError:
        return None

def to_iso(mon: str, day: int, year: int) -> str | None:
    try:
        return datetime(year, MONTH_NUM[mon], day).strftime("%Y-%m-%d")
    except (ValueError, KeyError):
        return None

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def normalize_deadline(raw: str) -> str | None:
    if not raw:
        return None
    raw = clean(raw)
    if re.search(r"closed|full|sold.?out|waitlist", raw, re.I):
        return "Registration Closed"
    if re.search(r"members?.only", raw, re.I):
        return "Members Only"
    m = re.search(r"open[s]?\s+(?:on\s+)?(.+)", raw, re.I)
    if m:
        d = fmt_date(m.group(1))
        if d:
            return f"Opens {d}"
    m = re.search(r"(?:clos(?:es?|ing)|deadline|due|by)[:\s]+(.+)", raw, re.I)
    if m:
        d = fmt_date(m.group(1))
        if d:
            return f"Closes {d}"
    if re.search(r"^(?:registration\s+)?open(?:\s+now)?$", raw, re.I):
        return "Registration Open"
    return fmt_date(raw)


# ── Data loading ──────────────────────────────────────────────────────────────
def load_tournament_list() -> list[dict]:
    import importlib.util
    spec = importlib.util.spec_from_file_location("tournament_list", TOUR_LIST_PATH)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.TOURNAMENTS

def load_existing_data() -> dict:
    if OUTPUT_PATH.exists():
        try:
            return {r["id"]: r for r in json.loads(OUTPUT_PATH.read_text()).get("tournaments", [])}
        except Exception:
            pass
    return {}

def load_pending() -> dict:
    if PENDING_PATH.exists():
        try:
            items = json.loads(PENDING_PATH.read_text())
            return {f"{p['tour']}::{p['name']}": p for p in items}
        except Exception:
            pass
    return {}


# ══════════════════════════════════════════════════════════════════════════════
# DGT — httpx, fully server-rendered
# ══════════════════════════════════════════════════════════════════════════════

DGT_SCHEDULE_URL = "https://www.dripgolftour.com/tournament"

def parse_dgt_row(row_text: str, reg_text: str, url: str) -> dict | None:
    """Parse one DGT schedule table row into a tournament dict."""
    # Dates: "April 18 - 19 Saturday - Sunday Radford, VA"
    dm = re.search(
        r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
        r"Dec(?:ember)?)\s+(\d{1,2})\s*-\s*(\d{1,2})?",
        row_text, re.I
    )
    if not dm:
        return None
    mon      = MONTH_ABBR.get(dm.group(1).lower())
    start_d  = int(dm.group(2))
    end_d    = int(dm.group(3)) if dm.group(3) else start_d
    start_iso = to_iso(mon, start_d, THIS_YEAR)
    end_iso   = to_iso(mon, end_d,   THIS_YEAR)
    if not start_iso or not mon:
        return None

    tid_m = re.search(r"TID=(\d+)", url)
    if not tid_m:
        return None

    name_m = re.search(r"TnmtName=([^&\s]+)", url)
    name   = name_m.group(1) if name_m else f"DGT Event"
    for enc, ch in [("%20"," "),("%22",'"'),("%27","'"),("%2C",","),("+"," ")]:
        name = name.replace(enc, ch)
    name = clean(name)

    # Location: text after day-of-week on first line
    loc_m    = re.search(r"(?:Sun|Mon|Tue|Wed|Thu|Fri|Sat)(?:urday|nesday|rsday|day)?\s+(.+?)(?:\n|$)", row_text, re.I)
    location = clean(loc_m.group(1)) if loc_m else ""

    state = "NC" if re.search(r"\bNC\b", location) else \
            "SC" if re.search(r"\bSC\b", location) else \
            "VA" if re.search(r"\bVA\b", location) else "Other"

    # Series
    series_m = re.search(r"(Elevated|Regional|Winter Series)", row_text, re.I)
    series   = series_m.group(1).capitalize() if series_m else "Open"

    # Deadline
    deadline = None
    t = clean(reg_text)
    if re.search(r"tournament full|sold.?out|closed", t, re.I):
        deadline = "Registration Closed"
    elif re.search(r"please login|login to register", t, re.I):
        deadline = "Members Only"
    else:
        # "Members M/D/YYYY at Xpm Non-Members M/D/YYYY at Xpm"
        mm = re.search(
            r"non-members\s+(\d{1,2}/\d{1,2}/\d{4})\s+at\s+(\d+(?:am|pm))",
            t, re.I
        )
        if mm:
            try:
                mo2, dy2, yr2 = map(int, mm.group(1).split("/"))
                dt2 = datetime(yr2, mo2, dy2)
                deadline = f"Opens {dt2.strftime(f'%b {dt2.day}, %Y')} (non-members)"
            except ValueError:
                pass
        if not deadline:
            cm = re.search(r"closes?\s+(\d{1,2}/\d{1,2}/\d{4})", t, re.I)
            if cm:
                try:
                    mo2, dy2, yr2 = map(int, cm.group(1).split("/"))
                    dt2 = datetime(yr2, mo2, dy2)
                    deadline = f"Closes {dt2.strftime(f'%b {dt2.day}, %Y')}"
                except ValueError:
                    pass
        if not deadline and re.search(r"registration open|open now", t, re.I):
            deadline = "Registration Open"

    return {
        "tid_num":   tid_m.group(1),
        "name":      name,
        "tour":      "DGT",
        "series":    series,
        "start":     start_iso,
        "end":       end_iso,
        "location":  location,
        "state":     state,
        "link":      url,
        "regDeadline": deadline,
    }


async def scrape_dgt(client: httpx.AsyncClient, known_ids: set, known_links: dict) -> tuple[list, list]:
    updates, discovered = [], []
    try:
        r = await client.get(DGT_SCHEDULE_URL, timeout=25)
        r.raise_for_status()
        html = r.text

        # Each row: [Info / Register](URL) <deadline text>
        rows = re.findall(
            r"\[Info / Register\]\((https://www\.dripgolftour\.com/Tournament/TournamentDetails[^)]+)\)(.*?)(?=\[Info / Register\]|\Z)",
            html, re.S
        )

        # Also grab the row text that precedes each [Info / Register] link
        # by splitting the full table
        table_rows = re.split(r"(?=\| \w+ \d+ - \d+|\| \w+ \d+ \w+day)", html)

        for url, reg_tail in rows:
            reg_text = clean(reg_tail.split("|")[0])
            # Find row context (date + location) by searching backwards in html
            pos = html.find(url)
            row_ctx = html[max(0, pos-500):pos]
            parsed = parse_dgt_row(row_ctx, reg_text, url)
            if not parsed:
                continue

            app_id = known_links.get(url) or known_links.get(url.split("&")[0])
            if app_id:
                entry = {"id": app_id}
                if parsed["regDeadline"]:
                    entry["regDeadline"] = parsed["regDeadline"]
                if parsed["start"]:
                    entry["start"] = parsed["start"]
                    entry["end"]   = parsed["end"]
                updates.append(entry)
            elif is_carolinas(parsed["location"]) or parsed["state"] in ("NC", "SC"):
                key = f"DGT::{parsed['name']}"
                discovered.append(parsed)

    except Exception as e:
        print(f"  ERROR DGT: {e}", file=sys.stderr)

    return updates, discovered


# ══════════════════════════════════════════════════════════════════════════════
# USKids — httpx, server-rendered
# ══════════════════════════════════════════════════════════════════════════════

USKIDS_LOCAL_TOURS = [
    "https://tournaments.uskidsgolf.com/tournaments/local/find-local-tour/507164/piedmont-triad-nc",
    "https://tournaments.uskidsgolf.com/tournaments/local/find-local-tour/496551/sandhills-nc",
]

async def scrape_uskids(client: httpx.AsyncClient, known_links: dict) -> tuple[list, list]:
    updates, discovered = [], []

    for tour_url in USKIDS_LOCAL_TOURS:
        try:
            await asyncio.sleep(DELAY)
            r = await client.get(tour_url, timeout=20)
            r.raise_for_status()
            html = r.text

            # Table rows: | [Course Name](url) | Date | Reg window | Late reg |
            event_rows = re.findall(
                r"\|\s*\[([^\]]+)\]\((https://tournaments\.uskidsgolf\.com/tournaments/local/find-tournament/\d+/[^)]+)\)"
                r"\s*\|\s*([^|]*?)\s*\|\s*([^|]*?)\s*\|\s*([^|]*?)\s*\|",
                html
            )

            for name, event_url, date_col, reg_col, late_col in event_rows:
                name      = clean(name)
                event_url = event_url.split("?")[0]
                date_col  = clean(date_col)
                reg_col   = clean(reg_col)

                # Parse reg window
                deadline = None
                if "Results" in reg_col or re.search(r"results", reg_col, re.I):
                    deadline = "Registration Closed"
                else:
                    # "Jan 20 - Apr 14" pattern
                    rng = re.search(r"(\w+ \d+)\s*-\s*(\w+ \d+|\d+)", reg_col)
                    if rng:
                        close_raw = rng.group(2).strip()
                        if re.match(r"^\d+$", close_raw):
                            mon_m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", rng.group(1), re.I)
                            if mon_m:
                                close_raw = f"{mon_m.group(1)} {close_raw}"
                        d = fmt_date(close_raw)
                        if d:
                            deadline = f"Closes {d}"
                    elif re.search(r"open", reg_col, re.I):
                        deadline = "Registration Open"

                app_id = known_links.get(event_url)
                if app_id:
                    entry = {"id": app_id}
                    if deadline:
                        entry["regDeadline"] = deadline
                    if date_col:
                        d = fmt_date(date_col)
                        if d:
                            entry["start"] = d
                            entry["end"]   = d
                    updates.append(entry)
                else:
                    state = "NC" if "nc" in tour_url.lower() else "SC"
                    discovered.append({
                        "source":      "USKids",
                        "name":        name,
                        "tour":        "USKids",
                        "series":      "Local",
                        "start":       fmt_date(date_col) or date_col,
                        "end":         fmt_date(date_col) or date_col,
                        "state":       state,
                        "link":        event_url,
                        "regDeadline": deadline or "",
                        "scraped_at":  TODAY,
                    })

        except Exception as e:
            print(f"  ERROR USKids {tour_url}: {e}", file=sys.stderr)

    return updates, discovered


# ══════════════════════════════════════════════════════════════════════════════
# Playwright scrapers
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_golfgenius_event(page, t: dict) -> dict:
    result = {"id": t["id"]}
    try:
        await page.goto(t["link"], wait_until="domcontentloaded", timeout=TIMEOUT)
        body = await page.inner_text("body")

        deadline = None
        if re.search(r"registration\s+(has\s+)?closed|event\s+is\s+full", body, re.I):
            deadline = "Registration Closed"
        else:
            om = re.search(r"registration\s+opens?\s+(?:on\s+)?((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,?\s*\d{4})?)", body, re.I)
            if om:
                d = fmt_date(om.group(1)); deadline = f"Opens {d}" if d else None
            if not deadline:
                cm = re.search(r"(?:registration\s+)?(?:closes?|deadline)\s+(?:on\s+)?((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,?\s*\d{4})?)", body, re.I)
                if cm:
                    d = fmt_date(cm.group(1)); deadline = f"Closes {d}" if d else None
            if not deadline and re.search(r"register\s+now|click\s+to\s+register", body, re.I):
                deadline = "Registration Open"
        if deadline:
            result["regDeadline"] = deadline

        if t.get("end", "") < TODAY:
            rows = await page.query_selector_all("table tr")
            parsed = []
            for row in rows[:30]:
                cells = await row.query_selector_all("td")
                if len(cells) < 3: continue
                texts = [clean(await c.inner_text()) for c in cells]
                if not re.match(r"^\d{1,3}[TE]?$", texts[0]): continue
                parsed.append({"place": texts[0], "name": texts[1], "score": texts[2]})
            if parsed:
                result["results"] = parsed[:20]
    except PWTimeout:
        print(f"  TIMEOUT: {t['link']}", file=sys.stderr)
    except Exception as e:
        print(f"  ERROR {t['id']}: {e}", file=sys.stderr)
    return result


async def scrape_hjgt_event(page, t: dict) -> dict:
    result = {"id": t["id"]}
    try:
        await page.goto(t["link"], wait_until="networkidle", timeout=TIMEOUT)
        body = await page.inner_text("body")

        deadline = None
        if re.search(r"registration\s+(is\s+)?closed|event\s+full|sold.?out", body, re.I):
            deadline = "Registration Closed"
        elif re.search(r"registration\s+not\s+yet\s+open|coming\s+soon", body, re.I):
            om = re.search(r"opens?\s+(?:on\s+)?((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,?\s*\d{4})?)", body, re.I)
            deadline = f"Opens {fmt_date(om.group(1))}" if om else "Registration Not Open Yet"
        else:
            dm = re.search(r"(?:registration\s+)?(?:deadline|closes?)[:\s]+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s*\d{4})?)", body, re.I)
            if dm:
                d = fmt_date(dm.group(1)); deadline = f"Closes {d}" if d else None
            elif re.search(r"register\s+now|register\s+online", body, re.I):
                deadline = "Registration Open"
        if deadline:
            result["regDeadline"] = deadline

        if t.get("end", "") < TODAY:
            rows = await page.query_selector_all(".leaderboard-table tr, .results tr, table tr")
            parsed = []
            for row in rows[:30]:
                cells = await row.query_selector_all("td")
                if len(cells) < 3: continue
                texts = [clean(await c.inner_text()) for c in cells]
                if not re.match(r"^\d{1,3}[TE]?$", texts[0]): continue
                parsed.append({"place": texts[0], "name": texts[1], "score": texts[2]})
            if parsed:
                result["results"] = parsed[:20]
    except PWTimeout:
        print(f"  TIMEOUT: {t['link']}", file=sys.stderr)
    except Exception as e:
        print(f"  ERROR {t['id']}: {e}", file=sys.stderr)
    return result


async def scrape_ajga_event(page, t: dict) -> dict:
    result = {"id": t["id"]}
    try:
        await page.goto(t["link"], wait_until="networkidle", timeout=TIMEOUT)
        body = await page.inner_text("body")

        deadline = None
        if re.search(r"registration\s+(is\s+)?closed|entry\s+(is\s+)?closed", body, re.I):
            deadline = "Registration Closed"
        else:
            dm = re.search(r"(?:entry|registration)\s+deadline[:\s]+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,?\s*\d{4})?)", body, re.I)
            if dm:
                deadline = fmt_date(dm.group(1))
            elif re.search(r"entry\s+(?:is\s+)?open|register\s+now", body, re.I):
                deadline = "Registration Open"
        if deadline:
            result["regDeadline"] = deadline

        if t.get("end", "") < TODAY:
            rows = await page.query_selector_all(".leaderboard tr, .results-table tr, table tr")
            parsed = []
            for row in rows[:30]:
                cells = await row.query_selector_all("td")
                if len(cells) < 3: continue
                texts = [clean(await c.inner_text()) for c in cells]
                if not re.match(r"^\d{1,3}[TE]?$", texts[0]): continue
                parsed.append({"place": texts[0], "name": texts[1], "score": texts[2]})
            if parsed:
                result["results"] = parsed[:20]
    except PWTimeout:
        print(f"  TIMEOUT: {t['link']}", file=sys.stderr)
    except Exception as e:
        print(f"  ERROR {t['id']}: {e}", file=sys.stderr)
    return result


async def scrape_pinehurst_event(page, t: dict) -> dict:
    result = {"id": t["id"]}
    try:
        await page.goto(t["link"], wait_until="networkidle", timeout=TIMEOUT)
        body = await page.inner_text("body")
        deadline = None
        if re.search(r"applications?\s+(are\s+)?closed|registration\s+closed", body, re.I):
            deadline = "Registration Closed"
        elif re.search(r"applications?\s+open|now\s+accepting", body, re.I):
            cm = re.search(r"applications?\s+(?:close|due|deadline)[:\s]+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,?\s*\d{4})?)", body, re.I)
            deadline = f"Closes {fmt_date(cm.group(1))}" if cm else "Registration Open"
        else:
            om = re.search(r"applications?\s+open[s]?\s+(?:on\s+)?((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,?\s*\d{4})?)", body, re.I)
            if om:
                d = fmt_date(om.group(1)); deadline = f"Opens {d}" if d else None
        if deadline:
            result["regDeadline"] = deadline
    except Exception as e:
        print(f"  ERROR {t['id']}: {e}", file=sys.stderr)
    return result


async def scrape_uskids_event(page, t: dict) -> dict:
    result = {"id": t["id"]}
    try:
        await page.goto(t["link"], wait_until="domcontentloaded", timeout=TIMEOUT)
        body = await page.inner_text("body")
        deadline = None
        if re.search(r"registration\s+(is\s+)?closed|sold.?out", body, re.I):
            deadline = "Registration Closed"
        else:
            cm = re.search(r"(?:registration\s+)?(?:closes?|deadline)[:\s]+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,?\s*\d{4})?)", body, re.I)
            if cm:
                d = fmt_date(cm.group(1)); deadline = f"Closes {d}" if d else None
            elif re.search(r"register\s+now|add\s+to\s+cart", body, re.I):
                deadline = "Registration Open"
        if deadline:
            result["regDeadline"] = deadline
        if t.get("end", "") < TODAY:
            rows = await page.query_selector_all("table tr")
            parsed = []
            for row in rows[:30]:
                cells = await row.query_selector_all("td")
                if len(cells) < 3: continue
                texts = [clean(await c.inner_text()) for c in cells]
                if not re.match(r"^\d{1,3}[TE]?$", texts[0]): continue
                parsed.append({"place": texts[0], "name": texts[1], "score": texts[2]})
            if parsed:
                result["results"] = parsed[:20]
    except Exception as e:
        print(f"  ERROR {t['id']}: {e}", file=sys.stderr)
    return result


# ── Discovery: HJGT schedule page ─────────────────────────────────────────────
async def discover_hjgt(page, known_tids: set) -> list[dict]:
    discovered = []
    try:
        await page.goto("https://tournaments.hjgt.org/Tournament", wait_until="networkidle", timeout=TIMEOUT)
        try:
            await page.wait_for_selector("table tr, .tournament-row", timeout=8000)
        except Exception:
            pass
        links = await page.eval_on_selector_all(
            "a[href*='TournamentDetails']",
            "els => els.map(e => ({href: e.href, row: e.closest('tr') ? e.closest('tr').innerText : ''}))"
        )
        for item in links:
            href = item.get("href", "")
            m    = re.search(r"TID=(\d+)", href)
            if not m or m.group(1) in known_tids:
                continue
            row_text = clean(item.get("row", ""))
            if not is_carolinas(row_text):
                continue
            state = "NC" if re.search(r"\bNC\b|North Carolina", row_text) else \
                    "SC" if re.search(r"\bSC\b|South Carolina", row_text) else "Other"
            discovered.append({
                "source": "HJGT", "name": row_text[:80], "tour": "HJGT",
                "series": "Open", "link": href, "state": state, "scraped_at": TODAY,
            })
    except Exception as e:
        print(f"  ERROR HJGT discovery: {e}", file=sys.stderr)
    return discovered


# ── Discovery: GolfGenius master pages ───────────────────────────────────────
GG_MASTER_SCHEDULES = [
    "https://cga.golfgenius.com/pages/3936922",
    "https://cga.golfgenius.com/pages/5774192",
    "https://cga.golfgenius.com/pages/5689125",
    "https://cga.golfgenius.com/pages/5721881",
]

async def discover_golfgenius(page, known_links: set) -> list[dict]:
    discovered = []
    for url in GG_MASTER_SCHEDULES:
        try:
            await page.goto(url, wait_until="networkidle", timeout=TIMEOUT)
            links = await page.eval_on_selector_all(
                "a[href*='/ggid/'], a[href*='golfgenius.com/pages/']",
                "els => els.map(e => ({href: e.href, text: e.innerText.trim()}))"
            )
            for lnk in links:
                href = lnk.get("href", "").split("?")[0]
                text = clean(lnk.get("text", ""))
                if not href or href in known_links or not text or len(text) < 5:
                    continue
                discovered.append({
                    "source": "GolfGenius", "name": text, "tour": "TYGA",
                    "series": "Open", "link": href, "state": "NC", "scraped_at": TODAY,
                })
        except Exception as e:
            print(f"  ERROR GG discovery {url}: {e}", file=sys.stderr)
        await asyncio.sleep(DELAY)
    return discovered


# ══════════════════════════════════════════════════════════════════════════════
# Router
# ══════════════════════════════════════════════════════════════════════════════

def get_source(t: dict) -> str:
    link = t.get("link", "")
    if "golfgenius.com" in link:                              return "golfgenius"
    if "hjgt.org/Tournament/TournamentDetails" in link:       return "hjgt"
    if "ajga.org/tournaments/" in link and "schedule" not in link and not link.endswith("/leaderboard"): return "ajga"
    if "uskidsgolf.com" in link and "/find-tournament/" in link: return "uskids"
    if "pinehurst.com/golf/tournaments/" in link:             return "pinehurst"
    return "skip"


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    all_tournaments = load_tournament_list()
    existing_by_id  = load_existing_data()
    pending_existing = load_pending()

    known_ids   = {t["id"] for t in all_tournaments}
    known_links = {t["link"]: t["id"] for t in all_tournaments if t.get("link")}
    known_tids  = {
        re.search(r"TID=(\d+)", t["link"]).group(1)
        for t in all_tournaments
        if t.get("link") and re.search(r"TID=(\d+)", t["link"])
    }

    results_by_id = {}
    all_discovered = []

    # ── httpx (no browser) ────────────────────────────────────────────────────
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/122.0.0.0 Safari/537.36"}
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:

        print("\n── DGT (server-rendered) ──")
        dgt_u, dgt_d = await scrape_dgt(client, known_ids, known_links)
        for u in dgt_u:
            results_by_id.setdefault(u["id"], {}).update(u)
        all_discovered.extend(dgt_d)
        print(f"  {len(dgt_u)} updates | {len(dgt_d)} new NC/SC events")

        print("\n── USKids (server-rendered) ──")
        usk_u, usk_d = await scrape_uskids(client, known_links)
        for u in usk_u:
            results_by_id.setdefault(u["id"], {}).update(u)
        all_discovered.extend(usk_d)
        print(f"  {len(usk_u)} updates | {len(usk_d)} new events")

    # ── Playwright ────────────────────────────────────────────────────────────
    pw_targets = [(t, get_source(t)) for t in all_tournaments if get_source(t) != "skip"]
    fn_map = {
        "golfgenius": scrape_golfgenius_event,
        "hjgt":       scrape_hjgt_event,
        "ajga":       scrape_ajga_event,
        "uskids":     scrape_uskids_event,
        "pinehurst":  scrape_pinehurst_event,
    }

    print(f"\n── Playwright: {len(pw_targets)} events ──")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}, locale="en-US",
        )
        page = await ctx.new_page()
        page.on("console", lambda _: None)

        for i, (t, source) in enumerate(pw_targets, 1):
            print(f"[{i}/{len(pw_targets)}] {t['id']} ({source})")
            scraped = await fn_map[source](page, t)
            if scraped:
                results_by_id.setdefault(t["id"], {}).update(scraped)
                if "regDeadline" in scraped: print(f"  deadline → {scraped['regDeadline']}")
                if "results"     in scraped: print(f"  results  → {len(scraped['results'])} rows")
            await asyncio.sleep(DELAY)

        # Discovery
        print("\n── Discovery: HJGT ──")
        hjgt_d = await discover_hjgt(page, known_tids)
        all_discovered.extend(hjgt_d)
        print(f"  {len(hjgt_d)} potential new NC/SC events")

        print("\n── Discovery: GolfGenius ──")
        gg_d = await discover_golfgenius(page, set(known_links.keys()))
        all_discovered.extend(gg_d)
        print(f"  {len(gg_d)} potential new events")

        await browser.close()

    # ── Write tournament_data.json ─────────────────────────────────────────────
    final = []
    for t in all_tournaments:
        merged = dict(existing_by_id.get(t["id"], {"id": t["id"]}))
        patch  = results_by_id.get(t["id"], {})
        for k, v in patch.items():
            if v is not None:
                merged[k] = v
        final.append(merged)

    OUTPUT_PATH.write_text(json.dumps({
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "tournaments": final,
    }, indent=2))

    print(f"\n✅ tournament_data.json: {len(final)} records | "
          f"{sum(1 for t in final if t.get('regDeadline'))} deadlines | "
          f"{sum(1 for t in final if t.get('results'))} with results")

    # ── Write new_tournaments_pending.json ─────────────────────────────────────
    added = 0
    for disc in all_discovered:
        key = f"{disc['tour']}::{disc['name']}"
        if key not in pending_existing and disc.get("link") not in known_links:
            pending_existing[key] = disc
            added += 1

    PENDING_PATH.write_text(json.dumps(list(pending_existing.values()), indent=2))
    print(f"✅ new_tournaments_pending.json: {len(pending_existing)} total | {added} new this run")
    if added:
        print("   → Review: new_tournaments_pending.json")


if __name__ == "__main__":
    asyncio.run(main())
