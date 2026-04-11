#!/usr/bin/env python3
"""
xB SoccerStats Scraper
-----------------------
Scrapes soccerstats.com for Championship half-time tables:
  - First half away  (GP, W, D, L, GF, GA, GD, Pts)
  - Second half away (same)
  - First half home  (same)
  - Second half home (same)

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

# ─────────────────────────────────────────────────────────────
# Playwright table extraction
# ─────────────────────────────────────────────────────────────

def scrape_halftime():
    """
    Returns dict with keys: first_half_away, second_half_away,
                            first_half_home, second_half_home
    Each is a list of team dicts.
    """
    from playwright.sync_api import sync_playwright

    results = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print(f"  Fetching: {TIMING_URL}")
        page.goto(TIMING_URL, wait_until="networkidle", timeout=30000)

        # The page has tabs -- click through each one and extract the table
        # Tab structure: "First Half away" / "Second Half away" on timing.asp
        # Half-time home/away tables are on halftime.asp

        # ── timing.asp: first/second half AWAY tables ──
        try:
            # First half away tab should be default
            page.wait_for_selector("table", timeout=10000)
            results["first_half_away"] = extract_table(page, tab_text="First Half away")
            results["second_half_away"] = extract_table(page, tab_text="Second Half away")
        except Exception as e:
            print(f"  Warning: timing.asp extraction failed: {e}")
            results["first_half_away"] = []
            results["second_half_away"] = []

        # ── halftime.asp: first/second half HOME tables ──
        try:
            halftime_url = "https://www.soccerstats.com/halftime.asp?league=england2"
            page.goto(halftime_url, wait_until="networkidle", timeout=30000)
            page.wait_for_selector("table", timeout=10000)
            results["first_half_home"]  = extract_table(page, tab_text="First Half home")
            results["second_half_home"] = extract_table(page, tab_text="Second Half home")
        except Exception as e:
            print(f"  Warning: halftime.asp extraction failed: {e}")
            results["first_half_home"]  = []
            results["second_half_home"] = []

        browser.close()

    return results


def extract_table(page, tab_text=None):
    """
    Click a tab (if specified), then extract the visible standings table.
    Returns list of dicts: {rank, name, gp, w, d, l, gf, ga, gd, pts}
    """
    if tab_text:
        try:
            # Find and click the tab
            tab = page.locator(f"text='{tab_text}'").first
            tab.click()
            page.wait_for_timeout(800)  # small wait for re-render
        except Exception:
            # Try partial match
            try:
                tabs = page.locator("a, td, th, button, span").all_text_contents()
                matched = [t for t in tabs if tab_text.lower() in t.lower()]
                if matched:
                    page.locator(f"text='{matched[0]}'").first.click()
                    page.wait_for_timeout(800)
            except Exception as e:
                print(f"    Tab click failed for '{tab_text}': {e}")

    rows = []
    try:
        # Find all tables and pick the one that looks like a standings table
        tables = page.locator("table").all()
        standings_table = None
        for table in tables:
            text = table.inner_text()
            # Standings tables have GP/W/D/L columns
            if any(col in text for col in ["GP", " W ", " D ", " L ", "Pts"]):
                if any(team in text for team in ["Birmingham", "Coventry", "Middlesbrough"]):
                    standings_table = table
                    break

        if not standings_table:
            print(f"    No standings table found for '{tab_text}'")
            return []

        # Extract rows
        trs = standings_table.locator("tr").all()
        for tr in trs:
            cells = [c.strip() for c in tr.all_text_contents()]
            # Filter: valid row has rank number, team name, numeric stats
            # Typical: ['1', 'Coventry City', '21', '12', '7', '2', '23', '7', '+16', '43']
            if len(cells) >= 9:
                try:
                    rank = int(cells[0])
                    name = cells[1]
                    # Try to parse GP
                    gp = int(cells[2])
                    rows.append({
                        "rank": rank,
                        "name": name,
                        "gp":   gp,
                        "w":    int(cells[3]),
                        "d":    int(cells[4]),
                        "l":    int(cells[5]),
                        "gf":   int(cells[6]),
                        "ga":   int(cells[7]),
                        "gd":   cells[8],   # keep as string e.g. "+16"
                        "pts":  int(cells[9]),
                    })
                except (ValueError, IndexError):
                    continue  # skip header rows and non-data rows

    except Exception as e:
        print(f"    Table extraction error for '{tab_text}': {e}")

    print(f"    Extracted {len(rows)} rows for '{tab_text}'")
    return rows


# ─────────────────────────────────────────────────────────────
# Change detection
# ─────────────────────────────────────────────────────────────

def snap_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def run(dry_run=False, force=False):
    print("Scraping soccerstats.com halftime tables...")
    data = scrape_halftime()

    # Summary
    for key, rows in data.items():
        print(f"  {key}: {len(rows)} teams")
        bham = next((r for r in rows if "birmingham" in r["name"].lower()), None)
        if bham:
            print(f"    Birmingham: rank {bham['rank']}, pts {bham['pts']}, "
                  f"W{bham['w']} D{bham['d']} L{bham['l']} "
                  f"GF{bham['gf']} GA{bham['ga']}")

    # Load existing
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

    # Update today or append
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
