#!/usr/bin/env python3
"""
xB SoccerStats Scraper
-----------------------
Scrapes soccerstats.com for Championship half-time tables:
  - First half away  (GP, W, D, L, GF, GA, GD, Pts)
  - Second half away
  - First half home
  - Second half home

Approach:
  - timing.asp serves first-half-away + second-half-away on one page.
  - halftime.asp serves first-half-home + second-half-home on one page.
  - Tables are plain HTML stacked vertically — no tabs to click.
  - Wait for a known team name (Birmingham) to be visible before extracting,
    so we know the content has rendered.
  - Parse the rendered body text by locating each section heading then
    walking forward consuming team rows.

Requirements:
    pip install playwright
    playwright install chromium

Usage:
    python scraper_halftime.py            # normal
    python scraper_halftime.py --dry-run  # inspect without writing
    python scraper_halftime.py --force    # write even if unchanged
    python scraper_halftime.py --debug    # save body text to disk for diagnosis
"""

import hashlib
import json
import os
import sys
from datetime import date

HALFTIME_PATH = os.path.join(os.path.dirname(__file__), "data", "halftime.json")
DEBUG_DIR     = os.path.join(os.path.dirname(__file__), "data", "debug")
TIMING_URL    = "https://www.soccerstats.com/timing.asp?league=england2"
HALFTIME_URL  = "https://www.soccerstats.com/halftime.asp?league=england2"

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

SECTION_HEADINGS = {
    "first_half_away":  "First Half away",
    "second_half_away": "Second Half away",
    "first_half_home":  "First Half at home",
    "second_half_home": "Second Half at home",
}


def canonicalise_name(soccerstats_name):
    return NAME_MAP.get(soccerstats_name, soccerstats_name)


def fetch_page_text(url, debug_label=None):
    """
    Load the URL via Playwright and return the full rendered body text.
    Waits for a Championship team name to be visible — that's our signal
    the standings tables have actually rendered, not just any page skeleton.
    """
    from playwright.sync_api import sync_playwright

    print(f"  Fetching: {url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # Wait for a known team name to appear in the rendered page text.
        # This is much more reliable than wait_for_selector("table") because
        # soccerstats.com has many small layout tables that load before content.
        try:
            page.wait_for_function(
                """() => {
                    const body = document.body ? document.body.innerText : '';
                    return body.includes('Birmingham City') &&
                           body.includes('Coventry City') &&
                           body.includes('Sheffield');
                }""",
                timeout=25000,
            )
        except Exception as e:
            print(f"    Warning: content marker wait timed out: {e}")

        # Small additional settle time
        page.wait_for_timeout(2000)

        body_text = page.locator("body").inner_text()

        # Debug dump if requested via --debug flag or env var
        if debug_label and ("--debug" in sys.argv or os.environ.get("XB_DEBUG")):
            os.makedirs(DEBUG_DIR, exist_ok=True)
            debug_path = os.path.join(DEBUG_DIR, f"{debug_label}.txt")
            with open(debug_path, "w") as f:
                f.write(body_text)
            print(f"    Debug: dumped body text to {debug_path}")

        browser.close()
    return body_text


def parse_section(body_text, heading, next_headings):
    """
    Find `heading` in body_text, then walk forward consuming team rows
    until we either run out of teams or hit one of the next_headings.

    A row in the rendered text is ten consecutive non-blank lines:
        rank, club, GP, W, D, L, GF, GA, GD, Pts
    """
    lines = [ln.strip() for ln in body_text.splitlines()]
    lines = [ln for ln in lines if ln != ""]

    # Match heading (case-insensitive, in case of weird casing)
    start = None
    for i, ln in enumerate(lines):
        if ln.lower() == heading.lower():
            start = i
            break

    if start is None:
        print(f"    Heading not found: '{heading}'")
        return []

    rows = []
    i = start + 1
    stop_set = {h.lower() for h in next_headings}

    while i < len(lines):
        if lines[i].lower() in stop_set:
            break

        if i + 9 >= len(lines):
            break

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
            gd  = stat_tokens[6]
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


def diagnostic_dump(body_text, label):
    """When parsing fails, print a sample of the body text so we can see why."""
    print(f"\n    === DIAGNOSTIC: {label} ===")
    if not body_text:
        print("    body_text is empty!")
        return

    print(f"    body_text length: {len(body_text)} chars")
    # Show first 500 chars and any line containing "Half"
    print("    First 400 chars:")
    print("    " + body_text[:400].replace("\n", " ⏎ "))

    print("\n    Lines containing 'Half':")
    for ln in body_text.splitlines():
        if "Half" in ln or "half" in ln:
            print(f"      {repr(ln.strip())}")

    print("\n    Lines containing 'Birmingham':")
    for ln in body_text.splitlines():
        if "Birmingham" in ln:
            print(f"      {repr(ln.strip())}")
    print("    === END DIAGNOSTIC ===\n")


def scrape_halftime():
    results = {
        "first_half_away":  [],
        "second_half_away": [],
        "first_half_home":  [],
        "second_half_home": [],
    }

    # AWAY page (timing.asp)
    try:
        away_text = fetch_page_text(TIMING_URL, debug_label="timing")
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
        if not results["first_half_away"] and not results["second_half_away"]:
            diagnostic_dump(away_text, "timing.asp")
    except Exception as e:
        print(f"  Warning: timing.asp scrape failed: {e}")

    # HOME page (halftime.asp)
    try:
        home_text = fetch_page_text(HALFTIME_URL, debug_label="halftime")
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
        if not results["first_half_home"] and not results["second_half_home"]:
            diagnostic_dump(home_text, "halftime.asp")
    except Exception as e:
        print(f"  Warning: halftime.asp scrape failed: {e}")

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
