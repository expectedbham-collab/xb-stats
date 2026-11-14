#!/usr/bin/env python3
"""
xB Championship Stats Scraper
Uses team-level data for most metrics -- verified 2026-04-11.
Auto-fetches session cookie via Playwright on each run.
"""

import hashlib, json, os, sys
from datetime import date

# The Analyst gives each season a new tournament ID. At season rollover this
# is the only line to change.
TMCL = "al48ooi8acoibema226051250"   # Championship 2026/27

STATS_URL     = f"https://theanalyst.com/wp-json/sdapi/v1/soccerdata/tournamentstats?tmcl={TMCL}"
STANDINGS_URL = f"https://theanalyst.com/wp-json/sdapi/v1/soccerdata/standings?tmcl={TMCL}"
XPTS_URL      = f"https://dataviz.theanalyst.com/project-data/soccer/{TMCL}/expected-points.json"
HISTORY_PATH  = os.path.join(os.path.dirname(__file__), "data", "history.json")

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://theanalyst.com/competition/english-championship/stats",
}


def fetch_all_via_browser():
    """Fetch stats/standings/xpts from inside the browser context, using the
    site's own session. Previous approach (observe network for a fresh
    x-sdapi-token, then replay via requests) was flaky -- the page doesn't
    always fire an sdapi request during the wait window, and without a
    fresh token any replay 401s. Doing the fetch inside the browser
    sidesteps the token problem entirely: the browser already has the
    right auth state; we just ask it to make the same requests the page
    itself would make."""
    from playwright.sync_api import sync_playwright

    print("  Launching browser...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=BASE_HEADERS["User-Agent"])
        page = context.new_page()

        try:
            page.goto(
                "https://theanalyst.com/competition/english-championship/stats",
                wait_until="domcontentloaded",
                timeout=60000,
            )
        except Exception as e:
            print(f"  Warning: page.goto raised {type(e).__name__}: {e}")

        # Small wait so the page's own JS has time to set any session-y
        # cookies before we start making requests.
        page.wait_for_timeout(3000)

        def fetch(label, url):
            print(f"  Fetching {label}...")
            resp = page.request.get(url, headers={
                "Accept": "application/json",
                "Referer": "https://theanalyst.com/competition/english-championship/stats",
            }, timeout=30000)
            if not resp.ok:
                raise RuntimeError(f"{label} fetch failed: HTTP {resp.status}")
            return resp.json()

        stats     = fetch("stats",     STATS_URL)
        standings = fetch("standings", STANDINGS_URL)
        xpts      = fetch("xPts",      XPTS_URL)

        browser.close()

    return stats, standings, xpts


def sf(v, fb=0.0):
    try: return float(v) if v is not None else fb
    except: return fb

def si(v, fb=0):
    try: return int(float(v)) if v is not None else fb
    except: return fb

def tname(r):
    return r.get("contestantName") or r.get("contestantShortName", "Unknown")

def get_section(raw, *path):
    try:
        d = raw["team"]
        for p in path:
            d = d[p]
        return d if isinstance(d, list) else []
    except:
        return []

def build_team_stats(raw):
    teams = {}

    def merge(name, d):
        if name not in teams:
            teams[name] = {}
        teams[name].update(d)

    for r in get_section(raw, "attack", "overall"):
        n = tname(r)
        merge(n, {
            "atk_played":       si(r.get("played")),
            "atk_goals":        si(r.get("goals")),
            "atk_xg":           round(sf(r.get("xg")), 2),
            "atk_shots":        si(r.get("total_shots")),
            "atk_shots_ot":     si(r.get("sot")),
            "atk_xg_per_shot":  round(sf(r.get("xg_per_shot")), 3),
            "atk_shot_conv":    round(sf(r.get("shot_conv")), 1),
            "atk_shots_in_box_perc": round(sf(r.get("shots_in_box_perc")), 1),
        })

    for r in get_section(raw, "attack", "non_pen"):
        n = tname(r)
        merge(n, {
            "np_goals":         si(r.get("np_goals")),
            "np_xg":            round(sf(r.get("team_np_xG")), 2),
            "np_shots":         si(r.get("np_shots")),
            "np_shots_ot":      si(r.get("np_sot")),
            "np_xg_per_shot":   round(sf(r.get("xg_per_shot")), 3),
        })

    for r in get_section(raw, "attack", "set_piece"):
        n = tname(r)
        merge(n, {
            "sp_atk_goals":     si(r.get("sp_goals")),
            "sp_atk_shots":     si(r.get("sp_shots")),
            "sp_atk_xg":        round(sf(r.get("team_sp_xG")), 2),
        })

    for r in get_section(raw, "attack", "misc"):
        n = tname(r)
        merge(n, {
            "atk_touches_box":      si(r.get("tch_in_box")),
            "atk_hit_woodwork":     si(r.get("hit_woodwork")),
            "atk_pens":             si(r.get("pens")),
            "atk_pen_goals":        si(r.get("pen_goals")),
            "atk_headed_goals":     si(r.get("headed_goals")),
            "atk_fast_break_shots": si(r.get("fast_break_shots")),
            "atk_fast_break_goals": si(r.get("fast_break_goals")),
        })

    for r in get_section(raw, "defending", "overall"):
        n = tname(r)
        merge(n, {
            "def_goals_con":        si(r.get("goals_against")),
            "def_xga":              round(sf(r.get("xg_against")), 2),
            "def_shots_faced":      si(r.get("total_shots_against")),
            "def_sot_faced":        si(r.get("sot_against")),
            "def_xga_per_shot":     round(sf(r.get("xg_per_shot_against")), 3),
            "def_shot_conv_against":round(sf(r.get("shot_conv_against")), 1),
        })

    for r in get_section(raw, "defending", "non_pen"):
        n = tname(r)
        merge(n, {
            "np_goals_against":     si(r.get("np_goals_against")),
            "np_xga":               round(sf(r.get("team_np_xG_against")), 2),
            "np_shots_against":     si(r.get("np_shots_against")),
            "np_sot_against":       si(r.get("np_sot_against")),
            "np_xga_per_shot":      round(sf(r.get("xg_per_shot_against")), 3),
        })

    for r in get_section(raw, "defending", "set_piece"):
        n = tname(r)
        merge(n, {
            "sp_def_goals_con": si(r.get("sp_goals_against")),
            "sp_def_xga":       round(sf(r.get("team_sp_xG_against")), 2),
        })

    for r in get_section(raw, "defending", "misc"):
        n = tname(r)
        merge(n, {
            "def_fast_break_shots_against": si(r.get("fast_break_shots_against")),
            "def_fast_break_goals_against": si(r.get("fast_break_goals_against")),
            "def_opp_touches_box":          si(r.get("opp_tch_in_box")),
        })

    for r in get_section(raw, "possession", "overall"):
        n = tname(r)
        merge(n, {
            "possession":           round(sf(r.get("pos_perc")), 1),
            "pass_passes":          si(r.get("passes")),
            "pass_success":         si(r.get("successful_pass")),
            "pass_pass_acc":        round(sf(r.get("accuracy")), 1),
            "pass_f3_passes":       si(r.get("final_third_passes")),
            "pass_f3_success":      si(r.get("successful_final_third_passes")),
            "pass_crosses":         si(r.get("op_crosses")),
            "pass_crosses_success": si(r.get("successful_op_crosses")),
            "pass_cross_acc":       round(sf(r.get("op_cross_accuracy_perc")), 1),
            "pass_through_balls":   si(r.get("through_balls")),
            "recoveries":           si(r.get("rec")),
            "tackles":              si(r.get("total_tackles")),
            "interceptions":        si(r.get("interceptions")),
            "ground_duel_pct":      round(sf(r.get("ground_duel_success_perc")), 1),
            "aerial_duel_pct":      round(sf(r.get("aerial_duel_success_perc")), 1),
            "blocks":               si(r.get("blocks")),
            "clearances":           si(r.get("clearances")),
        })

    for r in get_section(raw, "sequences", "overall"):
        n = tname(r)
        merge(n, {
            "press_ppda":               round(sf(r.get("ppda")), 2) if r.get("ppda") else None,
            "press_pressed_seqs":       si(r.get("pressed_sequences")),
            "press_high_to":            si(r.get("high_turnovers")),
            "press_high_to_shots":      si(r.get("shot_ending_high_turnovers")),
            "press_high_to_goals":      si(r.get("goal_ending_high_turnovers")),
            "press_press_start_dist":   round(sf(r.get("start_distance")), 1),
            "seq_direct_attacks":       si(r.get("direct_attacks")),
            "seq_direct_attack_goals":  si(r.get("direct_attack_goals")),
            "seq_buildups":             si(r.get("build_ups")),
            "seq_buildup_goals":        si(r.get("build_up_goals")),
            "seq_seqs_10plus":          si(r.get("ten_plus_passes")),
            "seq_direct_speed":         round(sf(r.get("direct_speed_for")), 2),
            "seq_passes_per_seq":       round(sf(r.get("passes_for")), 2),
        })

    for r in get_section(raw, "misc", "overall"):
        n = tname(r)
        merge(n, {
            "misc_yellows":         si(r.get("yellows")),
            "misc_reds":            si(r.get("reds")),
            "misc_fouls":           si(r.get("fouls_lost")),
            "misc_fouled":          si(r.get("fouls_won")),
            "misc_pens_won":        si(r.get("pens_won")),
            "misc_pens_conceded":   si(r.get("pens_conceded")),
            "misc_errors_shot":     si(r.get("errors_lead_to_shot")),
            "misc_errors_goal":     si(r.get("errors_lead_to_goal")),
        })

    # ── Self-computed non-penalty xG per shot (both sides) ──────────────
    # Opta's own non-penalty breakdown table (attack.non_pen / defending.non_pen)
    # isn't reliably available every week -- in particular the defending
    # half is often missing entirely early in a season, even when the
    # attacking half is populated. Rather than depend on Opta having
    # published that table, we back the penalty contribution out of the
    # totals we always have (overall xG/shots + penalty counts), so this
    # is guaranteed to be present every run regardless of what Opta has
    # gotten around to computing.
    #
    # PENALTY_XG is the assumed xG value of a single penalty attempt --
    # 0.79 is the widely used industry convention (FBref/Understat-style).
    # Adjust this constant if you'd rather use a different figure.
    PENALTY_XG = 0.79
    for name, s in teams.items():
        atk_shots = s.get("atk_shots")
        atk_pens  = s.get("atk_pens") or 0
        atk_xg    = s.get("atk_xg")
        if atk_shots is not None and atk_xg is not None:
            np_shots = atk_shots - atk_pens
            np_xg    = atk_xg - (PENALTY_XG * atk_pens)
            s["np_xg_per_shot"] = round(np_xg / np_shots, 3) if np_shots > 0 else None

        def_shots     = s.get("def_shots_faced")
        pens_conceded = s.get("misc_pens_conceded") or 0
        def_xga       = s.get("def_xga")
        if def_shots is not None and def_xga is not None:
            np_shots_against = def_shots - pens_conceded
            np_xga           = def_xga - (PENALTY_XG * pens_conceded)
            s["np_xga_per_shot"] = round(np_xga / np_shots_against, 3) if np_shots_against > 0 else None

    print(f"  Teams captured: {len(teams)}")
    return teams


def parse_standings(raw):
    result = {"total": [], "home": [], "away": []}
    try:
        for div in raw["stage"][0]["division"]:
            t = div.get("type")
            if t not in result: continue
            for r in div.get("ranking", []):
                result[t].append({
                    "rank":       r.get("rank"),
                    "lastRank":   r.get("lastRank"),
                    "rankStatus": r.get("rankStatus", ""),
                    "name":       r.get("contestantName"),
                    "shortName":  r.get("contestantShortName"),
                    "code":       r.get("contestantCode"),
                    "points":     r.get("points"),
                    "deduction":  r.get("deductionPoints", 0),
                    "played":     r.get("matchesPlayed"),
                    "won":        r.get("matchesWon"),
                    "drawn":      r.get("matchesDrawn"),
                    "lost":       r.get("matchesLost"),
                    "gf":         r.get("goalsFor"),
                    "ga":         r.get("goalsAgainst"),
                    "gd":         r.get("goaldifference"),
                    "lastSix":    r.get("lastSix", ""),
                })
    except (KeyError, IndexError) as e:
        print(f"  Standings parse error: {e}")
    return result


def parse_xpts(raw):
    result = []
    for r in raw.get("data", []):
        result.append({
            "name":      r.get("contestantName"),
            "shortName": r.get("contestantShortName"),
            "code":      r.get("contestantCode"),
            "played":    r.get("played"),
            "pos":       r.get("pos"),
            "xPos":      r.get("xPos"),
            "posDiff":   r.get("posDiff"),
            "points":    r.get("points"),
            "xPts":      round(sf(r.get("xPts")), 2),
            "ptsDiff":   round(sf(r.get("ptsDiff")), 2),
            "xG":        round(sf(r.get("xG")), 2),
            "xGA":       round(sf(r.get("xGA")), 2),
            "xGD":       round(sf(r.get("xG")) - sf(r.get("xGA")), 2),
            "deduction": r.get("points_deduction", 0),
        })
    result.sort(key=lambda x: x["xPos"] or 99)
    return result


# Every field the dashboard scores. If Opta renames one, sf()/si() quietly turn
# it into 0 for all 24 clubs, so check before anything is saved.
REQUIRED_FIELDS = ["atk_played", "atk_shots", "atk_shots_ot", "atk_xg", "atk_shots_in_box_perc",
                   "def_shots_faced", "def_sot_faced", "def_xga", "atk_fast_break_shots",
                   "seq_direct_attacks", "seq_buildups", "pass_through_balls", "pass_cross_acc",
                   "sp_atk_xg", "sp_def_xga", "press_ppda"]

def validate(teams):
    """Stop the run (non-zero exit, so GitHub emails you) if the payload is
    not a usable Championship table."""
    problems = []
    if len(teams) != 24:
        problems.append(f"expected 24 clubs, got {len(teams)}")
    for k in REQUIRED_FIELDS:
        vals = [t.get(k) for t in teams.values()]
        if not any(v for v in vals):
            problems.append(f"'{k}' is missing or zero for every club")
    if problems:
        sys.exit("Scrape rejected: " + "; ".join(problems))


def snap_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()

def infer_label(history, teams):
    # Label by the stats' own games-played count, not the standings. Opta
    # refreshes standings straight after a match but the stats a day or two
    # later, so on a Saturday or Sunday the standings are one match ahead of
    # the numbers this snapshot actually holds.
    from collections import Counter
    played_counts = [s.get("atk_played") for s in teams.values() if s.get("atk_played")]
    if played_counts:
        modal_gw = Counter(played_counts).most_common(1)[0][0]
        return f"GW{modal_gw}"
    return date.today().isoformat()


MIN_TEAMS_SAME_PLAYED = 16

def meets_threshold(teams, min_teams=MIN_TEAMS_SAME_PLAYED):
    from collections import Counter
    played_counts = [t.get("atk_played", 0) for t in teams.values() if t.get("atk_played")]
    if not played_counts:
        return False
    most_common_count = Counter(played_counts).most_common(1)[0][1]
    return most_common_count >= min_teams


def is_stale(stats_json, history):
    """True if Opta's stats are older than the newest snapshot we already hold.
    This is what happened on 2 Sept 2026: the API served last season's final
    table (last updated in July) and it overwrote the GW46 entry."""
    new = str(stats_json.get("team", {}).get("lastUpdated", ""))[:10]
    held = max((str(s.get("last_updated", ""))[:10] for s in history), default="")
    return bool(new and held and new < held)


def run(dry_run=False, force=False):
    print("Fetching data...")
    stats_json, standings_json, xpts_json = fetch_all_via_browser()

    teams     = build_team_stats(stats_json)
    validate(teams)
    standings = parse_standings(standings_json)
    xpts      = parse_xpts(xpts_json)

    print(f"  Standings rows: {len(standings['total'])} | xPts rows: {len(xpts)}")

    payload = {"teams": teams, "standings": standings, "xpts": xpts}

    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
    history = []
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH) as f:
            history = json.load(f)

    current_hash = snap_hash(payload)
    if current_hash == (history[-1].get("_hash") if history else None) and not force:
        print("No change since last snapshot.")
        open("/tmp/xb_no_change", "w").write("no_change")
        return

    if not force and is_stale(stats_json, history):
        print("Stats are older than the newest snapshot already saved. Skipping.")
        open("/tmp/xb_no_change", "w").write("no_change")
        return

    if not force and not meets_threshold(teams):
        from collections import Counter
        played_counts = [t.get("atk_played", 0) for t in teams.values() if t.get("atk_played")]
        dist = Counter(played_counts).most_common(3)
        print(f"Threshold not met -- games played distribution: {dist}")
        print(f"Fewer than {MIN_TEAMS_SAME_PLAYED} teams share the same games-played count. Skipping snapshot.")
        open("/tmp/xb_no_change", "w").write("no_change")
        return

    today = date.today().isoformat()
    # Identify the snapshot by gameweek (games played), not by today's date.
    # This is what actually dedupes runs: providers revise stats daily even
    # with no new match played, so hashing the whole payload alone creates a
    # new entry almost every run. Keying on the label instead means repeat
    # runs within the same gameweek refresh the existing entry in place, and
    # a new entry is only created when the games-played count really moves.
    label = infer_label(history, teams)
    snapshot = {
        "date": today,
        "label": label,
        "last_updated": stats_json.get("team", {}).get("lastUpdated", ""),
        "_hash": current_hash,
        **payload,
    }

    # Same label AND same 24 clubs: next August's GW1 must not overwrite this
    # season's GW1 if history.json has not been reset.
    existing = next((i for i, s in enumerate(history)
                     if s.get("label") == label and set(s.get("teams", {})) == set(teams)), None)
    if existing is not None:
        history[existing] = snapshot
        print(f"Updated snapshot for {label} ({today}) -- refreshed stats, same gameweek")
    else:
        history.append(snapshot)
        print(f"New snapshot: {label} ({today})")

    for name, stats in teams.items():
        if "birmingham" in name.lower():
            bx = next((x for x in xpts if "birmingham" in (x.get("name","")).lower()), {})
            print(f"\nBirmingham City:")
            print(f"  Pos: {bx.get('pos')} | xPos: {bx.get('xPos')} | Pts: {bx.get('points')} | xPts: {bx.get('xPts')}")
            print(f"  xG: {stats.get('atk_xg')} | SOT: {stats.get('atk_shots_ot')} | Fast break shots: {stats.get('atk_fast_break_shots')}")
            print(f"  Crosses: {stats.get('pass_crosses')} | Through balls: {stats.get('pass_through_balls')}")
            print(f"  PPDA: {stats.get('press_ppda')} | Recoveries: {stats.get('recoveries')}")
            print(f"  SP xG: {stats.get('sp_atk_xg')} | SP xGA: {stats.get('sp_def_xga')}")
            break

    if dry_run:
        print("\n[DRY RUN] Not writing.")
        return

    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2)
    print(f"\nDone. {len(history)} snapshots saved.")


if __name__ == "__main__":
    run(dry_run="--dry-run" in sys.argv, force="--force" in sys.argv)
