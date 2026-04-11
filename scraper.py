#!/usr/bin/env python3
"""
xB Championship Stats Scraper
--------------------------------
Three endpoints per run:
  1. tournamentstats  -- player-level stats aggregated to team totals
  2. standings        -- actual table (total / home / away)
  3. expected-points  -- xPts, xPos, posDiff, ptsDiff

Saves a snapshot only when data has changed since last run.
Run daily via GitHub Actions -- change detection means one snapshot per match.

Usage:
    python scraper.py            # normal
    python scraper.py --dry-run  # inspect without writing
    python scraper.py --force    # write even if unchanged
"""

import hashlib
import json
import os
import sys
from collections import defaultdict
from datetime import date

import requests

STATS_URL    = "https://theanalyst.com/wp-json/sdapi/v1/soccerdata/tournamentstats?tmcl=bmmk637l2a33h90zlu36kx8no"
STANDINGS_URL = "https://theanalyst.com/wp-json/sdapi/v1/soccerdata/standings?tmcl=bmmk637l2a33h90zlu36kx8no"
XPTS_URL     = "https://dataviz.theanalyst.com/project-data/soccer/bmmk637l2a33h90zlu36kx8no/expected-points.json"

HISTORY_PATH = os.path.join(os.path.dirname(__file__), "data", "history.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://theanalyst.com/",
}

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def sf(v, fb=0.0):
    try: return float(v) if v is not None else fb
    except: return fb

def si(v, fb=0):
    try: return int(float(v)) if v is not None else fb
    except: return fb

def get_players(raw, section, sub="overall"):
    try: return raw["player"][section][sub] or []
    except: return []

def tname(p):
    return p.get("contestantName") or p.get("contestantShortName", "Unknown")

def wavg(w, s):
    return round(s / w, 3) if w else None

RATE_KEYS = {
    "possession","pass_pass_acc","ground_duel_pct","aerial_duel_pct",
    "atk_xg_per_shot","np_xg_per_shot","def_xga_per_shot","press_ppda",
    "press_press_start_dist","seq_direct_speed","seq_passes_per_seq",
}

# ─────────────────────────────────────────────────────────────
# Stats aggregators
# ─────────────────────────────────────────────────────────────

def agg_attack(ps):
    t = defaultdict(lambda: dict(played=0,goals=0,xg=0.0,goals_vs_xg=0.0,shots=0,shots_ot=0,_xw=0.0,_cw=0.0))
    for p in ps:
        n=tname(p); d=t[n]
        apps=si(p.get("apps"))
        if apps>d["played"]: d["played"]=apps
        d["goals"]+=si(p.get("goals")); d["xg"]+=sf(p.get("xg"))
        d["goals_vs_xg"]+=sf(p.get("goals_vs_xg"))
        d["shots"]+=si(p.get("shots")); d["shots_ot"]+=si(p.get("shots_on_target"))
        s=si(p.get("shots"))
        d["_xw"]+=sf(p.get("xg_per_shot"))*s; d["_cw"]+=sf(p.get("shot_conv"))*s
    return {n:{"played":d["played"],"goals":d["goals"],"xg":round(d["xg"],2),
               "goals_vs_xg":round(d["goals_vs_xg"],2),"shots":d["shots"],
               "shots_ot":d["shots_ot"],"xg_per_shot":wavg(d["shots"],d["_xw"]),
               "shot_conv":wavg(d["shots"],d["_cw"])} for n,d in t.items()}

def agg_attack_sp(ps):
    t=defaultdict(lambda:dict(goals=0,shots=0,xg=0.0))
    for p in ps:
        n=tname(p); t[n]["goals"]+=si(p.get("goals")); t[n]["shots"]+=si(p.get("shots")); t[n]["xg"]+=sf(p.get("xg"))
    return {n:{"goals":d["goals"],"shots":d["shots"],"xg":round(d["xg"],2)} for n,d in t.items()}

def agg_attack_misc(ps):
    t=defaultdict(lambda:dict(pen_total=0,pen_goals=0,fk_total=0,fk_goals=0,
                               header_total=0,header_goals=0,fb_total=0,fb_goals=0,
                               touches_box=0,hit_post=0,offsides=0))
    for p in ps:
        n=tname(p); d=t[n]
        d["pen_total"]+=si(p.get("penalty_total")or p.get("pen_total")or p.get("penalties_total"))
        d["pen_goals"]+=si(p.get("penalty_goals")or p.get("pen_goals"))
        d["fk_total"]+=si(p.get("freekick_total")or p.get("fk_total")or p.get("free_kick_total"))
        d["fk_goals"]+=si(p.get("freekick_goals")or p.get("fk_goals"))
        d["header_total"]+=si(p.get("header_total")or p.get("headers_total"))
        d["header_goals"]+=si(p.get("header_goals"))
        d["fb_total"]+=si(p.get("fastbreak_total")or p.get("fast_break_total"))
        d["fb_goals"]+=si(p.get("fastbreak_goals")or p.get("fast_break_goals"))
        d["touches_box"]+=si(p.get("touches_in_box")or p.get("tou_in_box"))
        d["hit_post"]+=si(p.get("hit_post")or p.get("hitpost"))
        d["offsides"]+=si(p.get("offsides")or p.get("offside"))
    return {n:dict(d) for n,d in t.items()}

def agg_defence(ps):
    t=defaultdict(lambda:dict(goals_con=0,xga=0.0,shots_faced=0,sot_faced=0,_xw=0.0))
    for p in ps:
        n=tname(p); d=t[n]
        d["goals_con"]+=si(p.get("goals")or p.get("goals_conceded"))
        d["xga"]+=sf(p.get("xg")); d["shots_faced"]+=si(p.get("shots"))
        d["sot_faced"]+=si(p.get("shots_on_target")or p.get("sot"))
        d["_xw"]+=sf(p.get("xg_per_shot"))*si(p.get("shots"))
    return {n:{"goals_con":d["goals_con"],"xga":round(d["xga"],2),
               "shots_faced":d["shots_faced"],"sot_faced":d["sot_faced"],
               "xga_per_shot":wavg(d["shots_faced"],d["_xw"])} for n,d in t.items()}

def agg_defence_sp(ps):
    t=defaultdict(lambda:dict(goals=0,shots=0,xg=0.0))
    for p in ps:
        n=tname(p); t[n]["goals"]+=si(p.get("goals")); t[n]["shots"]+=si(p.get("shots")); t[n]["xg"]+=sf(p.get("xg"))
    return {n:{"goals_con":d["goals"],"shots_faced":d["shots"],"xga":round(d["xg"],2)} for n,d in t.items()}

def agg_def_actions(ps):
    t=defaultdict(lambda:dict(tackles=0,interceptions=0,recoveries=0,blocks=0,clearances=0,
                               _ps=0.0,_pn=0,_gs=0.0,_gn=0,_as=0.0,_an=0))
    for p in ps:
        n=tname(p); d=t[n]
        d["tackles"]+=si(p.get("tackles")or p.get("tackles_won"))
        d["interceptions"]+=si(p.get("interceptions")or p.get("ints"))
        d["recoveries"]+=si(p.get("recoveries")or p.get("recs"))
        d["blocks"]+=si(p.get("blocks")or p.get("blks"))
        d["clearances"]+=si(p.get("clearances")or p.get("clrs"))
        poss=p.get("possession")or p.get("avg_poss")
        if poss is not None: d["_ps"]+=sf(poss); d["_pn"]+=1
        gd=p.get("ground_duels_won_pct")or p.get("ground_duel_pct")
        if gd is not None: d["_gs"]+=sf(gd); d["_gn"]+=1
        ad=p.get("aerial_duels_won_pct")or p.get("aerial_duel_pct")
        if ad is not None: d["_as"]+=sf(ad); d["_an"]+=1
    return {n:{"tackles":d["tackles"],"interceptions":d["interceptions"],
               "recoveries":d["recoveries"],"blocks":d["blocks"],"clearances":d["clearances"],
               "possession":round(d["_ps"]/d["_pn"],1)if d["_pn"]else None,
               "ground_duel_pct":round(d["_gs"]/d["_gn"],1)if d["_gn"]else None,
               "aerial_duel_pct":round(d["_as"]/d["_an"],1)if d["_an"]else None} for n,d in t.items()}

def agg_passing(ps):
    t=defaultdict(lambda:dict(passes=0,passes_success=0,_pw=0.0,f3=0,f3s=0,crosses=0,crosses_s=0,through_balls=0))
    for p in ps:
        n=tname(p); d=t[n]
        tot=si(p.get("passes_total")or p.get("total_passes")or p.get("passes"))
        d["passes"]+=tot; d["passes_success"]+=si(p.get("passes_successful")or p.get("successful_passes"))
        d["_pw"]+=sf(p.get("pass_accuracy")or p.get("pass_acc"))*tot
        d["f3"]+=si(p.get("passes_final_third_total")or p.get("final_third_passes"))
        d["f3s"]+=si(p.get("passes_final_third_successful"))
        d["crosses"]+=si(p.get("crosses_total")or p.get("crosses"))
        d["crosses_s"]+=si(p.get("crosses_successful"))
        d["through_balls"]+=si(p.get("through_balls")or p.get("throughballs")or p.get("through_passes"))
    return {n:{"passes":d["passes"],"passes_success":d["passes_success"],
               "pass_acc":wavg(d["passes"],d["_pw"]),"f3_passes":d["f3"],
               "f3_success":d["f3s"],"crosses":d["crosses"],"crosses_success":d["crosses_s"],
               "through_balls":d["through_balls"]} for n,d in t.items()}

def agg_pressing(ps):
    t=defaultdict(lambda:dict(pressed_seqs=0,_ps=0.0,_pn=0,_ss=0.0,_sn=0,high_to=0,high_to_shots=0,high_to_goals=0))
    for p in ps:
        n=tname(p); d=t[n]
        d["pressed_seqs"]+=si(p.get("pressed_sequences")or p.get("pressed_seqs"))
        ppda=p.get("ppda")
        if ppda is not None: d["_ps"]+=sf(ppda); d["_pn"]+=1
        sd=p.get("start_distance")or p.get("press_start_distance")
        if sd is not None: d["_ss"]+=sf(sd); d["_sn"]+=1
        d["high_to"]+=si(p.get("high_turnovers")or p.get("high_turnover_total"))
        d["high_to_shots"]+=si(p.get("high_turnover_shots")or p.get("shot_ending"))
        d["high_to_goals"]+=si(p.get("high_turnover_goals")or p.get("goal_ending"))
    return {n:{"pressed_seqs":d["pressed_seqs"],
               "ppda":round(d["_ps"]/d["_pn"],1)if d["_pn"]else None,
               "press_start_dist":round(d["_ss"]/d["_sn"],1)if d["_sn"]else None,
               "high_to":d["high_to"],"high_to_shots":d["high_to_shots"],"high_to_goals":d["high_to_goals"]} for n,d in t.items()}

def agg_sequences(ps):
    t=defaultdict(lambda:dict(seqs_10plus=0,_ds=0.0,_dn=0,_pp=0.0,_pn=0,_st=0.0,_tn=0,buildups=0,bu_goals=0,direct_attacks=0,da_goals=0))
    for p in ps:
        n=tname(p); d=t[n]
        d["seqs_10plus"]+=si(p.get("sequences_10plus")or p.get("10_plus_passes"))
        ds=p.get("direct_speed")
        if ds is not None: d["_ds"]+=sf(ds); d["_dn"]+=1
        pps=p.get("passes_per_sequence")or p.get("passes_per_seq")
        if pps is not None: d["_pp"]+=sf(pps); d["_pn"]+=1
        st=p.get("sequence_time")
        if st is not None: d["_st"]+=sf(st); d["_tn"]+=1
        d["buildups"]+=si(p.get("buildups")or p.get("build_ups"))
        d["bu_goals"]+=si(p.get("buildup_goals")or p.get("build_up_goals"))
        d["direct_attacks"]+=si(p.get("direct_attacks"))
        d["da_goals"]+=si(p.get("direct_attack_goals"))
    return {n:{"seqs_10plus":d["seqs_10plus"],
               "direct_speed":round(d["_ds"]/d["_dn"],2)if d["_dn"]else None,
               "passes_per_seq":round(d["_pp"]/d["_pn"],2)if d["_pn"]else None,
               "seq_time":round(d["_st"]/d["_tn"],2)if d["_tn"]else None,
               "buildups":d["buildups"],"bu_goals":d["bu_goals"],
               "direct_attacks":d["direct_attacks"],"da_goals":d["da_goals"]} for n,d in t.items()}

def agg_misc(ps):
    t=defaultdict(lambda:dict(subs=0,subs_goals=0,errors_shot=0,errors_goal=0,
                               fouled=0,yellows=0,reds=0,pens_won=0,fouls=0,opp_yellows=0,opp_reds=0))
    for p in ps:
        n=tname(p); d=t[n]
        d["subs"]+=si(p.get("subs_used")or p.get("substitutions_used"))
        d["subs_goals"]+=si(p.get("subs_goals")or p.get("sub_goals"))
        d["errors_shot"]+=si(p.get("errors_lead_to_shot")or p.get("error_lead_to_shot"))
        d["errors_goal"]+=si(p.get("errors_lead_to_goal")or p.get("error_lead_to_goal"))
        d["fouled"]+=si(p.get("fouled")); d["yellows"]+=si(p.get("yellow_cards")or p.get("yellows"))
        d["reds"]+=si(p.get("red_cards")or p.get("reds"))
        d["pens_won"]+=si(p.get("penalties_won")or p.get("pens_won"))
        d["fouls"]+=si(p.get("fouls")); d["opp_yellows"]+=si(p.get("opp_yellow_cards")or p.get("opp_yellows"))
        d["opp_reds"]+=si(p.get("opp_red_cards")or p.get("opp_reds"))
    return {n:dict(d) for n,d in t.items()}

SECTIONS = [
    ("attack","overall",   agg_attack,     "atk_"),
    ("attack","nonpenalty",agg_attack,     "np_"),
    ("attack","setpieces", agg_attack_sp,  "sp_atk_"),
    ("attack","misc",      agg_attack_misc,"atk_misc_"),
    ("defence","overall",  agg_defence,    "def_"),
    ("defence","nonpenalty",agg_defence,   "def_np_"),
    ("defence","setpieces",agg_defence_sp, "sp_def_"),
    ("defence","actions",  agg_def_actions,""),
    ("passing","overall",  agg_passing,    "pass_"),
    ("pressing","overall", agg_pressing,   "press_"),
    ("sequences","overall",agg_sequences,  "seq_"),
    ("misc","overall",     agg_misc,       "misc_"),
]

def build_team_stats(raw):
    merged = {}
    found = []
    for section, sub, fn, prefix in SECTIONS:
        ps = get_players(raw, section, sub)
        if not ps: continue
        found.append(f"{section}.{sub}")
        for team, stats in fn(ps).items():
            if team not in merged: merged[team] = {}
            for k, v in stats.items():
                merged[team][f"{prefix}{k}"] = v
    print(f"  Stats sections: {', '.join(found)}")
    return merged

# ─────────────────────────────────────────────────────────────
# Standings
# ─────────────────────────────────────────────────────────────

def parse_standings(raw):
    result = {"total": [], "home": [], "away": []}
    try:
        divisions = raw["stage"][0]["division"]
        for div in divisions:
            t = div.get("type")
            if t not in result: continue
            for r in div.get("ranking", []):
                result[t].append({
                    "rank":        r.get("rank"),
                    "lastRank":    r.get("lastRank"),
                    "rankStatus":  r.get("rankStatus", ""),
                    "name":        r.get("contestantName"),
                    "shortName":   r.get("contestantShortName"),
                    "code":        r.get("contestantCode"),
                    "points":      r.get("points"),
                    "deduction":   r.get("deductionPoints", 0),
                    "played":      r.get("matchesPlayed"),
                    "won":         r.get("matchesWon"),
                    "drawn":       r.get("matchesDrawn"),
                    "lost":        r.get("matchesLost"),
                    "gf":          r.get("goalsFor"),
                    "ga":          r.get("goalsAgainst"),
                    "gd":          r.get("goaldifference"),
                    "lastSix":     r.get("lastSix", ""),
                })
    except (KeyError, IndexError) as e:
        print(f"  Warning: standings parse error: {e}")
    return result

# ─────────────────────────────────────────────────────────────
# Expected points
# ─────────────────────────────────────────────────────────────

def parse_xpts(raw):
    result = []
    for r in raw.get("data", []):
        result.append({
            "name":        r.get("contestantName"),
            "shortName":   r.get("contestantShortName"),
            "code":        r.get("contestantCode"),
            "played":      r.get("played"),
            "pos":         r.get("pos"),
            "xPos":        r.get("xPos"),
            "posDiff":     r.get("posDiff"),
            "points":      r.get("points"),
            "xPts":        round(sf(r.get("xPts")), 2),
            "ptsDiff":     round(sf(r.get("ptsDiff")), 2),
            "xG":          round(sf(r.get("xG")), 2),
            "xGA":         round(sf(r.get("xGA")), 2),
            "xGD":         round(sf(r.get("xG")) - sf(r.get("xGA")), 2),
            "deduction":   r.get("points_deduction", 0),
        })
    result.sort(key=lambda x: x["xPos"] or 99)
    return result

# ─────────────────────────────────────────────────────────────
# Change detection + label
# ─────────────────────────────────────────────────────────────

def snap_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()

def infer_label(history, teams):
    for name, stats in teams.items():
        if "birmingham" in name.lower():
            played = stats.get("atk_played")
            if played: return f"GW {played}"
    if not history: return "GW 1"
    last = history[-1].get("label", "GW 0")
    try: return f"GW {int(last.split()[-1]) + 1}"
    except: return f"GW {len(history) + 1}"

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def run(dry_run=False, force=False):
    session = requests.Session()
    session.headers.update(HEADERS)

    print("1/3 Fetching stats...")
    r1 = session.get(STATS_URL, timeout=30); r1.raise_for_status()
    raw_stats = r1.json()

    print("2/3 Fetching standings...")
    r2 = session.get(STANDINGS_URL, timeout=30); r2.raise_for_status()
    raw_standings = r2.json()

    print("3/3 Fetching expected points...")
    r3 = session.get(XPTS_URL, timeout=30); r3.raise_for_status()
    raw_xpts = r3.json()

    teams     = build_team_stats(raw_stats)
    standings = parse_standings(raw_standings)
    xpts      = parse_xpts(raw_xpts)

    print(f"  Teams (stats): {len(teams)}")
    print(f"  Standings rows: {len(standings['total'])}")
    print(f"  xPts rows: {len(xpts)}")

    payload = {"teams": teams, "standings": standings, "xpts": xpts}

    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
    history = []
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH) as f:
            history = json.load(f)

    current_hash = snap_hash(payload)
    last_hash = history[-1].get("_hash") if history else None

    if current_hash == last_hash and not force:
        print("No change since last snapshot -- nothing to do.")
        with open("/tmp/xb_no_change", "w") as f: f.write("no_change")
        return

    today   = date.today().isoformat()
    label   = infer_label(history, teams)
    last_updated = raw_stats.get("player", {}).get("lastUpdated", "")

    snapshot = {
        "date": today,
        "label": label,
        "last_updated": last_updated,
        "_hash": current_hash,
        **payload,
    }

    existing = next((i for i,s in enumerate(history) if s.get("date")==today), None)
    if existing is not None:
        snapshot["label"] = history[existing]["label"]
        history[existing] = snapshot
        print(f"Updated today's snapshot ({snapshot['label']})")
    else:
        history.append(snapshot)
        print(f"New snapshot: {today} ({label})")

    # Birmingham summary
    for name, stats in teams.items():
        if "birmingham" in name.lower():
            bham_x = next((x for x in xpts if "birmingham" in (x.get("name","")).lower()), {})
            print(f"\nBirmingham City:")
            print(f"  Actual pos: {bham_x.get('pos')} | xPos: {bham_x.get('xPos')} | posDiff: {bham_x.get('posDiff')}")
            print(f"  Points: {bham_x.get('points')} | xPts: {bham_x.get('xPts')} | ptsDiff: {bham_x.get('ptsDiff')}")
            print(f"  xG: {stats.get('atk_xg')} | xGA: {stats.get('def_xga')} | Possession: {stats.get('possession')}")
            break

    if dry_run:
        print("\n[DRY RUN] Not writing.")
        return

    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2)
    print(f"\nDone. {len(history)} snapshots saved.")


if __name__ == "__main__":
    run(dry_run="--dry-run" in sys.argv, force="--force" in sys.argv)
