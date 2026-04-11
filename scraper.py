#!/usr/bin/env python3
"""
xB Championship Stats Scraper
Uses team-level data for most metrics -- verified 2026-04-11.
"""

import hashlib, json, os, sys
from datetime import date
import requests

STATS_URL     = "https://theanalyst.com/wp-json/sdapi/v1/soccerdata/tournamentstats?tmcl=bmmk637l2a33h90zlu36kx8no"
STANDINGS_URL = "https://theanalyst.com/wp-json/sdapi/v1/soccerdata/standings?tmcl=bmmk637l2a33h90zlu36kx8no"
XPTS_URL      = "https://dataviz.theanalyst.com/project-data/soccer/bmmk637l2a33h90zlu36kx8no/expected-points.json"
HISTORY_PATH  = os.path.join(os.path.dirname(__file__), "data", "history.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://theanalyst.com/competition/english-championship/stats",
    "x-sdapi-token": "LRkJ2MjwlC8RxUfVkne4",
}

def sf(v, fb=0.0):
    try: return float(v) if v is not None else fb
    except: return fb

def si(v, fb=0):
    try: return int(float(v)) if v is not None else fb
    except: return fb

def tname(r):
    return r.get("contestantName") or r.get("contestantShortName", "Unknown")

def get_section(raw, *path):
    """Safely get raw['team'][path[0]][path[1]] etc."""
    try:
        d = raw["team"]
        for p in path:
            d = d[p]
        return d or []
    except:
        return []

def build_team_stats(raw):
    teams = {}

    def merge(name, d):
        if name not in teams:
            teams[name] = {}
        teams[name].update(d)

    # ── attack.overall ──────────────────────────────────────────
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

    # ── attack.non_pen ──────────────────────────────────────────
    for r in get_section(raw, "attack", "non_pen"):
        n = tname(r)
        merge(n, {
            "np_goals":         si(r.get("np_goals")),
            "np_xg":            round(sf(r.get("team_np_xG")), 2),
            "np_shots":         si(r.get("np_shots")),
            "np_shots_ot":      si(r.get("np_sot")),
            "np_xg_per_shot":   round(sf(r.get("xg_per_shot")), 3),
        })

    # ── attack.set_piece ────────────────────────────────────────
    for r in get_section(raw, "attack", "set_piece"):
        n = tname(r)
        merge(n, {
            "sp_atk_goals":     si(r.get("sp_goals")),
            "sp_atk_shots":     si(r.get("sp_shots")),
            "sp_atk_xg":        round(sf(r.get("team_sp_xG")), 2),
        })

    # ── attack.misc (FAST BREAKS here) ──────────────────────────
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

    # ── defending.overall ───────────────────────────────────────
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

    # ── defending.set_piece ─────────────────────────────────────
    for r in get_section(raw, "defending", "set_piece"):
        n = tname(r)
        merge(n, {
            "sp_def_goals_con": si(r.get("sp_goals_against")),
            "sp_def_xga":       round(sf(r.get("team_sp_xG_against")), 2),
        })

    # ── defending.misc (FAST BREAKS AGAINST) ────────────────────
    for r in get_section(raw, "defending", "misc"):
        n = tname(r)
        merge(n, {
            "def_fast_break_shots_against": si(r.get("fast_break_shots_against")),
            "def_fast_break_goals_against": si(r.get("fast_break_goals_against")),
            "def_opp_touches_box":          si(r.get("opp_tch_in_box")),
        })

    # ── possession.overall (CROSSES, PASSES, RECOVERIES) ────────
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

    # ── sequences.overall (PPDA, PRESSING, DIRECT ATTACKS) ──────
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

    # ── misc.overall (FOULS, CARDS, ERRORS) ─────────────────────
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

    found = [k for k in ["attack","possession","defending","sequences","misc"] 
             if any(k in str(list(teams.values())[:1]))]
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


def snap_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()

def infer_label(history, teams):
    for name, stats in teams.items():
        if "birmingham" in name.lower():
            p = stats.get("atk_played")
            if p: return f"GW {p}"
    if not history: return "GW 1"
    last = history[-1].get("label", "GW 0")
    try: return f"GW {int(last.split()[-1]) + 1}"
    except: return f"GW {len(history) + 1}"


def run(dry_run=False, force=False):
    session = requests.Session()
    session.headers.update(HEADERS)

    print("1/3 Fetching stats...")
    r1 = session.get(STATS_URL, timeout=30); r1.raise_for_status()
    print("2/3 Fetching standings...")
    r2 = session.get(STANDINGS_URL, timeout=30); r2.raise_for_status()
    print("3/3 Fetching expected points...")
    r3 = session.get(XPTS_URL, timeout=30); r3.raise_for_status()

    teams     = build_team_stats(r1.json())
    standings = parse_standings(r2.json())
    xpts      = parse_xpts(r3.json())

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

    today = date.today().isoformat()
    label = infer_label(history, teams)
    snapshot = {
        "date": today,
        "label": label,
        "last_updated": r1.json().get("team", {}).get("lastUpdated", ""),
        "_hash": current_hash,
        **payload,
    }

    idx = next((i for i, s in enumerate(history) if s.get("date") == today), None)
    if idx is not None:
        snapshot["label"] = history[idx]["label"]
        history[idx] = snapshot
        print(f"Updated today's snapshot ({snapshot['label']})")
    else:
        history.append(snapshot)
        print(f"New snapshot: {today} ({label})")

    # Birmingham summary
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
