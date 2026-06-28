#!/usr/bin/env python3
"""
xB SoccerStats Scraper
-----------------------
Scrapes soccerstats.com for Championship half-time tables:
  - First half away  (GP, W, D, L, GF, GA, GD, Pts)
  - Second half away (same)
  - First half home  (same)
  - Second half home (same)

Approach:
  - timing.asp serves BOTH first-half-away and second-half-away tables on one page.
  - halftime.asp serves BOTH first-half-home and second-half-home tables on one page.
  - Both tables on each page are rendered as plain HTML (no interactive tabs).
  - We grab the rendered page text and parse out the four standings tables by
    locating each section heading then walking forward to consume team rows.

Saves to data/halftime.json alongside history.json.
Runs after scraper.py in the GitHub Actions workflow.

Requirements:
    pip install playwright
    playwright install chromium

Usage:
    python scraper_halftime.py            # normal
    python scraper_halftime.py --dry-run  # inspect without writing
    python scraper_halftime.py --force    # write even if unchanged
"""

import hashlib
import json
import os
import sys
from datetime import date

HALFTIME_PATH = os.path.join(os.path.dirname(__file__), "data", "halftime.json")
TIMING_URL    = "https://www.soccerstats.com/timing.asp?league=england2"
HALFTIME_URL  = "https://www.soccerstats.com/halftime.asp?league=england2"

# Championship clubs we're looking for, using the short names soccerstats.com uses.
CHAMPIONSHIP_TEAMS_SS = {
    "Birmingham City", "Blackburn", "Bristol City", "Charlton", "Coventry City",
    "Derby County", "Hull City", "Ipswich Town", "Leicester City", "Middlesbrough",
    "Millwall", "Norwich City", "Oxford Utd", "Portsmouth", "Preston",
    "QP Rangers", "Sheffield Utd", "Sheffield Wed", "Southampton", "Stoke City",
    "Swansea City", "Watford", "West Brom", "Wrexham",
}

# Mapping from soccerstats.com short forms to the canonical names used elsewhere
# in the project (these match the names in the dashboard's HT_NAME map).
NAME_MAP = {
    "QP Rangers":    "QPR",
    "Sheffield Utd": "Sheffield United",
    "Sheffield Wed": "Sheffield Wednesday",
    "Oxford Utd":    "Oxford United",
    "Blackburn":     "Blackburn Rovers",
    "Charlton":      "Charlton Athletic",
}

SECTION_HEADINGS = {
    "first_half_away":  "First Half away",
    "second_half_away": "Second Half away",
    "first_half_home":  "First Half at home",
    "second_half_home": "Second Half at home",
}


def canonicalise_name(soccerstats_name):
    return NAME_MAP.get(soccerstats_name, soccerstats_name)


def fetch_page_text(url):
    """Load the URL via Playwright and return the full rendered body text."""
    from playwright.sync_api import sync_playwright

    print(f"  Fetching: {url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(1500)  # let any late JS settle
        body_text = page.locator("body").inner_text()
        browser.close()
    return body_text


def parse_section(body_text, heading, next_headings):
    """
    Find `heading` in body_text, then walk forward consuming team rows
    until we either run out of teams or hit one of the next_headings.

    A row in the rendered text looks like ten consecutive non-blank lines:
        rank, club, GP, W, D, L, GF, GA, GD, Pts
    """
    lines = [ln.strip() for ln in body_text.splitlines()]
    lines = [ln for ln in lines if ln != ""]

    try:
        start = next(i for i, ln in enumerate(lines) if ln == heading)
    except StopIteration:
        print(f"    Heading not found: '{heading}'")
        return []

    rows = []
    i = start + 1
    stop_set = set(next_headings)

    while i < len(lines):
        if lines[i] in stop_set:
            break

        if i + 9 >= len(lines):
            i += 1
            continue

        rank_token = lines[i]
        club_token = lines[i + 1]

        if not rank_token.isdigit():
            i += 1
            continue
        rank_val = int(rank_token)
        if rank_val < 1 or rank_val > 30:
            i += 1
            continue

        if club_token not in CHAMPIONSHIP_TEAMS_SS:
            i += 1
            continue

        stat_tokens = lines[i + 2 : i + 10]
        if len(stat_tokens) < 8:
            i += 1
            continue

        try:
            gp  = int(stat_tokens[0])
            w   = int(stat_tokens[1])
            d   = int(stat_tokens[2])
            l   = int(stat_tokens[3])
            gf  = int(stat_tokens[4])
            ga  = int(stat_tokens[5])
            gd  = stat_tokens[6]  # may have +/-/0
            pts = int(stat_tokens[7])
        except ValueError:
            i += 1
            continue

        if gp < 1 or gp > 50:
            i += 1
            continue

        rows.append({
            "rank": rank_val,
            "name": canonicalise_name(club_token),
            "gp":   gp,
            "w":    w,
            "d":    d,
            "l":    l,
            "gf":   gf,
            "ga":   ga,
            "gd":   gd,
            "pts":  pts,
        })

        i += 10

    return rows


def scrape_halftime():
    """
    Returns dict with keys: first_half_away, second_half_away,
                            first_half_home, second_half_home
    """
    results = {
        "first_half_away":  [],
        "second_half_away": [],
        "first_half_home":  [],
        "second_half_home": [],
    }

    # AWAY page (timing.asp)
    try:
        away_text = fetch_page_text(TIMING_URL)
        results["first_half_away"] = parse_section(
            away_text,
            SECTION_HEADINGS["first_half_away"],
            next_headings=[SECTION_HEADINGS["second_half_away"]],
        )
        results["second_half_away"] = parse_section(
            away_text,
            SECTION_HEADINGS["second_half_away"],
            next_headings=[],
        )
        print(f"    first_half_away: {len(results['first_half_away'])} rows")
        print(f"    second_half_away: {len(results['second_half_away'])} rows")
    except Exception as e:
        print(f"  Warning: timing.asp scrape failed: {e}")

    # HOME page (halftime.asp)
    try:
        home_text = fetch_page_text(HALFTIME_URL)
        results["first_half_home"] = parse_section(
            home_text,
            SECTION_HEADINGS["first_half_home"],
            next_headings=[SECTION_HEADINGS["second_half_home"]],
        )
        results["second_half_home"] = parse_section(
            home_text,
            SECTION_HEADINGS["second_half_home"],
            next_headings=[],
        )
        print(f"    first_half_home: {len(results['first_half_home'])} rows")
        print(f"    second_half_home: {len(results['second_half_home'])} rows")
    except Exception as e:
        print(f"  Warning: halftime.asp scrape failed: {e}")

    return results


def snap_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()


def run(dry_run=False, force=False):
    print("Scraping soccerstats.com halftime tables...")
    data = scrape_halftime()

    # Summary including Birmingham detail
    for key, rows in data.items():
        print(f"  {key}: {len(rows)} teams")
        bham = next((r for r in rows if "birmingham" in r["name"].lower()), None)
        if bham:
            print(f"    Birmingham: rank {bham['rank']}, pts {bham['pts']}, "
                  f"W{bham['w']} D{bham['d']} L{bham['l']} "
                  f"GF{bham['gf']} GA{bham['ga']}")

    # Safety: refuse to overwrite halftime.json if every section came back empty
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
