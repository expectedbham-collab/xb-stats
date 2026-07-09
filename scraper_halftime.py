#!/usr/bin/env python3
"""
xB SoccerStats Halftime Scraper
--------------------------------
Scrapes soccerstats.com for Championship half-time tables:
  - First half away  (GP, W, D, L, GF, GA, GD, Pts)
  - Second half away
  - First half home
  - Second half home

Hybrid approach:
  1. Playwright fetches the rendered HTML — handles the cookie/privacy banner
     by clicking through any visible accept/dismiss button.
  2. BeautifulSoup parses the HTML for standings tables. The tables are
     server-side rendered into the page; once we have the HTML, parsing is
     deterministic and fast.

Saves to data/halftime.json alongside history.json.

Requirements:
    pip install playwright beautifulsoup4
    playwright install chromium

Usage:
    python scraper_halftime.py            # normal
    python scraper_halftime.py --dry-run  # inspect without writing
    python scraper_halftime.py --force    # write even if unchanged
    python scraper_halftime.py --debug    # dump fetched HTML to disk
"""

import hashlib
import json
import os
import sys
from datetime import date

from bs4 import BeautifulSoup

HALFTIME_PATH = os.path.join(os.path.dirname(__file__), "data", "halftime.json")
DEBUG_DIR     = os.path.join(os.path.dirname(__file__), "data", "debug")
TIMING_URL    = "https://www.soccerstats.com/timing.asp?league=england2"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

CHAMPIONSHIP_TEAMS_SS = {
    "Birmingham City", "Blackburn", "Bristol City", "Charlton", "Coventry City",
    "Derby County", "Hull City", "Ipswich Town", "Leicester City", "Middlesbrough",
    "Millwall", "Norwich City", "Oxford Utd", "Portsmouth", "Preston",
    "QP Rangers", "Sheffield Utd", "Sheffield Wed", "Southampton", "Stoke City",
    "Swansea City", "Watford", "West Brom", "Wrexham",
}

NAME_MAP = {
    "QP Rangers":    "QPR",
    "Sheffield Utd": "Sheffield United",
    "Sheffield Wed": "Sheffield Wednesday",
    "Oxford Utd":    "Oxford United",
    "Blackburn":     "Blackburn Rovers",
    "Charlton":      "Charlton Athletic",
}

SECTION_MARKERS = {
    "first_half_away":  "First Half table (AWAY, 1-45 min.)",
    "second_half_away": "Second Half table (AWAY, 46-90 min.)",
    "first_half_home":  "First Half table (AT HOME, 1-45 min.)",
    "second_half_home": "Second Half table (AT HOME, 46-90 min.)",
}


def canonicalise_name(soccerstats_name):
    return NAME_MAP.get(soccerstats_name, soccerstats_name)


def fetch_html_via_browser(url):
    """Use Playwright to fetch the rendered HTML, handling consent banners."""
    from playwright.sync_api import sync_playwright

    print(f"  Fetching: {url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 900},
            locale="en-GB",
            timezone_id="Europe/London",
        )

        # Hide the navigator.webdriver flag — some sites use it for bot detection
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        page = context.new_page()

        # Warm-up: visit homepage first so cookies / session state look natural
        try:
            page.goto("https://www.soccerstats.com/",
                      wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(1500)
        except Exception as e:
            print(f"    Warm-up failed (non-fatal): {e}")

        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # Try to dismiss any consent / cookie banner
        dismiss_consent(page)

        # Wait for the actual data to appear
        try:
            page.wait_for_function(
                """() => {
                    const t = document.body ? document.body.innerText : '';
                    return t.includes('Birmingham City') &&
                           t.includes('Coventry City') &&
                           t.includes('First Half table (AWAY');
                }""",
                timeout=25000,
            )
        except Exception as e:
            print(f"    Content wait timed out: {e}")

        page.wait_for_timeout(1500)
        html = page.content()
        browser.close()

    return html


def dismiss_consent(page):
    """Try a series of common consent-banner button selectors."""
    candidates = [
        "button:has-text('Accept All')",
        "button:has-text('Accept all')",
        "button:has-text('I Accept')",
        "button:has-text('Accept')",
        "button:has-text('Agree')",
        "button:has-text('Continue')",
        "button:has-text('Got it')",
        "button:has-text('OK')",
        "#cmpwelcomebtnyes",
        "[id*='accept']",
        "[class*='accept']",
        "[class*='consent'] button",
    ]
    for sel in candidates:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=1000):
                el.click(timeout=3000)
                print(f"    Dismissed consent via: {sel}")
                page.wait_for_timeout(800)
                return
        except Exception:
            continue


# ─────────────────────────────────────────────────────────────
# HTML parsing (deterministic once we have the page)
# ─────────────────────────────────────────────────────────────

def is_standings_table(table):
    text = table.get_text(" ", strip=True)
    has_columns = all(col in text for col in ["GP", "GF", "GA", "Pts"])
    has_team    = any(t in text for t in ("Birmingham City", "Coventry City", "Sheffield"))
    return has_columns and has_team


def parse_standings_table(table):
    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 10:
            continue
        if not cells[0].isdigit():
            continue
        rank_val = int(cells[0])
        if rank_val < 1 or rank_val > 30:
            continue

        club_token = cells[1]
        if club_token not in CHAMPIONSHIP_TEAMS_SS:
            continue

        try:
            gp  = int(cells[2])
            w   = int(cells[3])
            d   = int(cells[4])
            l   = int(cells[5])
            gf  = int(cells[6])
            ga  = int(cells[7])
            gd  = cells[8]
            pts = int(cells[9])
        except ValueError:
            continue

        if gp < 1 or gp > 50:
            continue

        rows.append({
            "rank": rank_val,
            "name": canonicalise_name(club_token),
            "gp":   gp, "w": w, "d": d, "l": l,
            "gf":   gf, "ga": ga, "gd": gd, "pts": pts,
        })
    return rows


def find_table_after_marker(soup, marker_text):
    """Find the marker text in the document, then walk forward to the next
    standings-shaped table."""
    marker_node = soup.find(string=lambda s: s and marker_text in s)
    if marker_node is None:
        return None
    current = marker_node
    while True:
        current = current.find_next("table")
        if current is None:
            return None
        if is_standings_table(current):
            return current


def scrape_halftime():
    results = {
        "first_half_away":  [],
        "second_half_away": [],
        "first_half_home":  [],
        "second_half_home": [],
    }

    try:
        html = fetch_html_via_browser(TIMING_URL)
    except Exception as e:
        print(f"  Failed to fetch {TIMING_URL}: {e}")
        return results

    # Optional debug dump
    if "--debug" in sys.argv or os.environ.get("XB_DEBUG"):
        os.makedirs(DEBUG_DIR, exist_ok=True)
        debug_path = os.path.join(DEBUG_DIR, "timing.html")
        with open(debug_path, "w") as f:
            f.write(html)
        print(f"    Debug: dumped HTML to {debug_path}")

    soup = BeautifulSoup(html, "html.parser")

    for key, marker in SECTION_MARKERS.items():
        table = find_table_after_marker(soup, marker)
        if table is None:
            print(f"    {key}: marker not found ('{marker}')")
            continue
        rows = parse_standings_table(table)
        results[key] = rows
        print(f"    {key}: {len(rows)} rows")

    return results


def snap_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()


def run(dry_run=False, force=False):
    print("Scraping soccerstats.com halftime tables...")
    data = scrape_halftime()

    for key, rows in data.items():
        print(f"  {key}: {len(rows)} teams")
        bham = next((r for r in rows if "birmingham" in r["name"].lower()), None)
        if bham:
            print(f"    Birmingham: rank {bham['rank']}, pts {bham['pts']}, "
                  f"W{bham['w']} D{bham['d']} L{bham['l']} "
                  f"GF{bham['gf']} GA{bham['ga']}")

    if not any(data.values()):
        print("  All sections empty — refusing to overwrite halftime.json with empty data.")
        return False

    os.makedirs(os.path.dirname(HALFTIME_PATH), exist_ok=True)
    history = []
    if os.path.exists(HALFTIME_PATH):
        with open(HALFTIME_PATH) as f:
            history = json.load(f)

    current_hash = snap_hash(data)
    last_hash = history[-1].get("_hash") if history else None

    if current_hash == last_hash and not force:
        print("No change in halftime data -- nothing to do.")
        return False

    today = date.today().isoformat()
    snapshot = {
        "date": today,
        "_hash": current_hash,
        **data,
    }

    existing = next((i for i, s in enumerate(history) if s.get("date") == today), None)
    if existing is not None:
        history[existing] = snapshot
        print(f"Updated today's halftime snapshot ({today})")
    else:
        history.append(snapshot)
        print(f"New halftime snapshot: {today} ({len(history)} total)")

    if dry_run:
        print("\n[DRY RUN] Not writing.")
        return False

    with open(HALFTIME_PATH, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Saved to {HALFTIME_PATH}")
    return True


if __name__ == "__main__":
    run(dry_run="--dry-run" in sys.argv, force="--force" in sys.argv)
