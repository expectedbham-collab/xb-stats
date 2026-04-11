#!/usr/bin/env python3
"""
xB Championship Stats Scraper
Field names verified against live API 2026-04-10.
"""

import hashlib, json, os, sys
from collections import defaultdict
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

def tname(p): return p.get("contestantName") or p.get("contestantShortName","Unknown")
def wavg(w, s): return round(s/w,3) if w else None
def players(raw, sec, sub):
    try: return raw["player"][sec][sub] or []
    except: return []

def agg_attack_overall(ps):
    t = defaultdict(lambda:dict(played=0,goals=0,xg=0.0,goals_vs_xg=0.0,shots=0,shots_ot=0,_xw=0.0))
    for p in ps:
        n=tname(p); d=t[n]
        apps=si(p.get("apps"))
        if apps>d["played"]: d["played"]=apps
        d["goals"]      +=si(p.get("goals"))
        d["xg"]         +=sf(p.get("xg"))
        d["goals_vs_xg"]+=sf(p.get("goals_vs_xg"))
        d["shots"]      +=si(p.get("shots"))
        d["shots_ot"]   +=si(p.get("shots_on_target"))
        s=si(p.get("shots")); d["_xw"]+=sf(p.get("xg_per_shot"))*s
    return {n:{"atk_played":d["played"],"atk_goals":d["goals"],"atk_xg":round(d["xg"],2),
               "atk_goals_vs_xg":round(d["goals_vs_xg"],2),"atk_shots":d["shots"],
               "atk_shots_ot":d["shots_ot"],"atk_xg_per_shot":wavg(d["shots"],d["_xw"])}
            for n,d in t.items()}

def agg_attack_np(ps):
    t = defaultdict(lambda:dict(np_goals=0,np_xg=0.0,np_shots=0,np_shots_ot=0,_xw=0.0))
    for p in ps:
        n=tname(p); d=t[n]
        d["np_goals"]   +=si(p.get("np_goals"))
        d["np_xg"]      +=sf(p.get("np_xg"))
        d["np_shots"]   +=si(p.get("np_shots"))
        d["np_shots_ot"]+=si(p.get("np_shots_on_target"))
        s=si(p.get("np_shots")); d["_xw"]+=sf(p.get("np_xg_per_shot"))*s
    return {n:{"np_goals":d["np_goals"],"np_xg":round(d["np_xg"],2),
               "np_shots":d["np_shots"],"np_shots_ot":d["np_shots_ot"],
               "np_xg_per_shot":wavg(d["np_shots"],d["_xw"])}
            for n,d in t.items()}

def agg_passing(ps):
    t = defaultdict(lambda:dict(passes=0,passes_s=0,_pw=0.0,f3=0,crosses=0,crosses_s=0,tb=0,tb_s=0))
    for p in ps:
        n=tname(p); d=t[n]
        tot=si(p.get("passes"))
        d["passes"]  +=tot; d["passes_s"]+=si(p.get("successful_passes"))
        d["_pw"]     +=sf(p.get("pass_perc"))*tot
        d["f3"]      +=si(p.get("total_final_third_passes"))
        d["crosses"]  +=si(p.get("op_crosses")); d["crosses_s"]+=si(p.get("successful_op_crosses"))
        d["tb"]      +=si(p.get("through_balls")); d["tb_s"]+=si(p.get("successful_through_balls"))
    return {n:{"pass_passes":d["passes"],"pass_pass_acc":wavg(d["passes"],d["_pw"]),
               "pass_f3_passes":d["f3"],"pass_crosses":d["crosses"],
               "pass_crosses_success":d["crosses_s"],"pass_through_balls":d["tb"],
               "pass_through_balls_success":d["tb_s"]}
            for n,d in t.items()}

def agg_chance_creation(ps):
    t = defaultdict(lambda:dict(cc=0,xa=0.0,assists=0))
    for p in ps:
        n=tname(p); d=t[n]
        d["cc"]     +=si(p.get("chances_created"))
        d["xa"]     +=sf(p.get("xa"))
        d["assists"]+=si(p.get("assists"))
    return {n:{"cc_chances_created":d["cc"],"cc_xa":round(d["xa"],2),"cc_assists":d["assists"]}
            for n,d in t.items()}

def agg_carries(ps):
    t = defaultdict(lambda:dict(carries=0,prog=0,shot_end=0,goal_end=0))
    for p in ps:
        n=tname(p); d=t[n]
        d["carries"] +=si(p.get("carries"))
        d["prog"]    +=si(p.get("progressive_carries"))
        d["shot_end"]+=si(p.get("shot_ending"))
        d["goal_end"]+=si(p.get("goal_ending"))
    return {n:{"carry_carries":d["carries"],"carry_progressive":d["prog"],
               "carry_shot_ending":d["shot_end"],"carry_goal_ending":d["goal_end"]}
            for n,d in t.items()}

def agg_defending(ps):
    t = defaultdict(lambda:dict(tack=0,ints=0,rec=0,blk=0,clr=0,
                                gd=0,gdw=0,ad=0,adw=0,_gn=0,_gs=0.0,_an=0,_as=0.0))
    for p in ps:
        n=tname(p); d=t[n]
        d["tack"]+=si(p.get("tackles")); d["ints"]+=si(p.get("interceptions"))
        d["rec"] +=si(p.get("recoveries")); d["blk"]+=si(p.get("blocks"))
        d["clr"] +=si(p.get("clearances"))
        d["gd"]  +=si(p.get("ground_duels")); d["gdw"]+=si(p.get("ground_duels_won"))
        d["ad"]  +=si(p.get("aerial_duels")); d["adw"]+=si(p.get("aerial_duels_won"))
        g=p.get("ground_duel_perc")
        if g is not None: d["_gs"]+=sf(g); d["_gn"]+=1
        a=p.get("aerial_duel_perc")
        if a is not None: d["_as"]+=sf(a); d["_an"]+=1
    return {n:{"def_tackles":d["tack"],"def_interceptions":d["ints"],
               "recoveries":d["rec"],"def_blocks":d["blk"],"def_clearances":d["clr"],
               "def_ground_duels":d["gd"],"def_ground_duels_won":d["gdw"],
               "def_aerial_duels":d["ad"],"def_aerial_duels_won":d["adw"],
               "ground_duel_pct":round(d["_gs"]/d["_gn"],1) if d["_gn"] else None,
               "aerial_duel_pct":round(d["_as"]/d["_an"],1) if d["_an"] else None}
            for n,d in t.items()}

def agg_discipline(ps):
    t = defaultdict(lambda:dict(y=0,r=0,f=0,pc=0,off=0))
    for p in ps:
        n=tname(p); d=t[n]
        d["y"]  +=si(p.get("yellows")); d["r"]+=si(p.get("reds"))
        d["f"]  +=si(p.get("fouls_commited")); d["pc"]+=si(p.get("pens_conceded"))
        d["off"]+=si(p.get("offsides"))
    return {n:{"misc_yellows":d["y"],"misc_reds":d["r"],"misc_fouls":d["f"],
               "misc_pens_conceded":d["pc"],"misc_offsides":d["off"]}
            for n,d in t.items()}

def agg_goalkeeping(ps):
    t = defaultdict(lambda:dict(gc=0,sv=0,xgot=0.0,gp=0.0))
    for p in ps:
        n=tname(p); d=t[n]
        d["gc"]  +=si(p.get("goals_conceded")); d["sv"]+=si(p.get("saves_made"))
        d["xgot"]+=sf(p.get("xgot_conceded")); d["gp"] +=sf(p.get("goals_prevented"))
    return {n:{"gk_goals_conceded":d["gc"],"gk_saves":d["sv"],
               "gk_xgot_conceded":round(d["xgot"],2),"gk_goals_prevented":round(d["gp"],2)}
            for n,d in t.items()}

SECTIONS=[
    ("attack","overall",agg_attack_overall),
    ("attack","nonPenalty",agg_attack_np),
    ("possession","passing",agg_passing),
    ("possession","chanceCreation",agg_chance_creation),
    ("carries","overall",agg_carries),
    ("defending","overall",agg_defending),
    ("defending","discipline",agg_discipline),
    ("goalkeeping","overall",agg_goalkeeping),
]

def build_team_stats(raw):
    merged={}; found=[]
    for sec,sub,fn in SECTIONS:
        ps=players(raw,sec,sub)
        if not ps: continue
        found.append(f"{sec}.{sub}")
        for team,stats in fn(ps).items():
            if team not in merged: merged[team]={}
            merged[team].update(stats)
    print(f"  Stats sections: {', '.join(found)}")
    return merged

def parse_standings(raw):
    result={"total":[],"home":[],"away":[]}
    try:
        for div in raw["stage"][0]["division"]:
            t=div.get("type")
            if t not in result: continue
            for r in div.get("ranking",[]):
                result[t].append({
                    "rank":r.get("rank"),"lastRank":r.get("lastRank"),
                    "rankStatus":r.get("rankStatus",""),"name":r.get("contestantName"),
                    "shortName":r.get("contestantShortName"),"code":r.get("contestantCode"),
                    "points":r.get("points"),"deduction":r.get("deductionPoints",0),
                    "played":r.get("matchesPlayed"),"won":r.get("matchesWon"),
                    "drawn":r.get("matchesDrawn"),"lost":r.get("matchesLost"),
                    "gf":r.get("goalsFor"),"ga":r.get("goalsAgainst"),
                    "gd":r.get("goaldifference"),"lastSix":r.get("lastSix",""),
                })
    except (KeyError,IndexError) as e: print(f"  Standings parse error: {e}")
    return result

def parse_xpts(raw):
    result=[]
    for r in raw.get("data",[]):
        result.append({
            "name":r.get("contestantName"),"shortName":r.get("contestantShortName"),
            "code":r.get("contestantCode"),"played":r.get("played"),
            "pos":r.get("pos"),"xPos":r.get("xPos"),"posDiff":r.get("posDiff"),
            "points":r.get("points"),"xPts":round(sf(r.get("xPts")),2),
            "ptsDiff":round(sf(r.get("ptsDiff")),2),
            "xG":round(sf(r.get("xG")),2),"xGA":round(sf(r.get("xGA")),2),
            "xGD":round(sf(r.get("xG"))-sf(r.get("xGA")),2),
            "deduction":r.get("points_deduction",0),
        })
    result.sort(key=lambda x:x["xPos"] or 99)
    return result

def snap_hash(data): return hashlib.md5(json.dumps(data,sort_keys=True).encode()).hexdigest()

def infer_label(history,teams):
    for name,stats in teams.items():
        if "birmingham" in name.lower():
            p=stats.get("atk_played")
            if p: return f"GW {p}"
    if not history: return "GW 1"
    last=history[-1].get("label","GW 0")
    try: return f"GW {int(last.split()[-1])+1}"
    except: return f"GW {len(history)+1}"

def run(dry_run=False,force=False):
    session=requests.Session(); session.headers.update(HEADERS)
    print("1/3 Fetching stats...")
    r1=session.get(STATS_URL,timeout=30); r1.raise_for_status()
    print("2/3 Fetching standings...")
    r2=session.get(STANDINGS_URL,timeout=30); r2.raise_for_status()
    print("3/3 Fetching expected points...")
    r3=session.get(XPTS_URL,timeout=30); r3.raise_for_status()

    teams=build_team_stats(r1.json())
    standings=parse_standings(r2.json())
    xpts=parse_xpts(r3.json())
    print(f"  Teams (stats): {len(teams)} | Standings rows: {len(standings['total'])} | xPts rows: {len(xpts)}")

    payload={"teams":teams,"standings":standings,"xpts":xpts}
    os.makedirs(os.path.dirname(HISTORY_PATH),exist_ok=True)
    history=[]
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH) as f: history=json.load(f)

    current_hash=snap_hash(payload)
    if current_hash==(history[-1].get("_hash") if history else None) and not force:
        print("No change since last snapshot.")
        open("/tmp/xb_no_change","w").write("no_change"); return

    today=date.today().isoformat()
    label=infer_label(history,teams)
    snapshot={"date":today,"label":label,
              "last_updated":r1.json().get("player",{}).get("lastUpdated",""),
              "_hash":current_hash,**payload}

    idx=next((i for i,s in enumerate(history) if s.get("date")==today),None)
    if idx is not None:
        snapshot["label"]=history[idx]["label"]; history[idx]=snapshot
        print(f"Updated today's snapshot ({snapshot['label']})")
    else:
        history.append(snapshot); print(f"New snapshot: {today} ({label})")

    for name,stats in teams.items():
        if "birmingham" in name.lower():
            bx=next((x for x in xpts if "birmingham" in (x.get("name","")).lower()),{})
            print(f"\nBirmingham City:")
            print(f"  Pos: {bx.get('pos')} | xPos: {bx.get('xPos')} | Pts: {bx.get('points')} | xPts: {bx.get('xPts')}")
            print(f"  xG: {stats.get('atk_xg')} | SOT: {stats.get('atk_shots_ot')} | Tackles: {stats.get('def_tackles')} | Recoveries: {stats.get('recoveries')}")
            print(f"  Passes: {stats.get('pass_passes')} | Through balls: {stats.get('pass_through_balls')} | Crosses: {stats.get('pass_crosses')}")
            break

    if dry_run: print("\n[DRY RUN] Not writing."); return
    with open(HISTORY_PATH,"w") as f: json.dump(history,f,indent=2)
    print(f"\nDone. {len(history)} snapshots saved.")

if __name__=="__main__":
    run(dry_run="--dry-run" in sys.argv,force="--force" in sys.argv)
