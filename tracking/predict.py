#!/usr/bin/env python3
"""xB Index prediction tracker.

Locks Index calls for upcoming fixtures and scores them once results are in.

  python tracking/predict.py call  DATE "Home|Away" ["Home|Away" ...]
      Adds one row per fixture using the latest snapshot in data/history.json.
      Odds can be added inline: "Home|Away|2.40|3.25|3.10" (decimal home/draw/away).

  python tracking/predict.py result DATE "Home|Away|HG|AG" [...]
      Fills in the score for an existing row.

  python tracking/predict.py summary
      Prints the running record and writes tracking/summary.md.

Rules come from the November 2026 backtest (132 matches, 28 Aug to 31 Oct).
"""
import csv, json, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HIST = os.path.join(ROOT, "data", "history.json")
CSV = os.path.join(ROOT, "tracking", "predictions.csv")
SUMMARY = os.path.join(ROOT, "tracking", "summary.md")

COLS = ["date", "home", "away", "snapshot", "home_idx", "away_idx", "gap",
        "away_side_away_ppg", "call", "confidence",
        "odds_h", "odds_d", "odds_a", "mkt_h", "mkt_d", "mkt_a", "mkt_prob_of_call",
        "hg", "ag", "outcome", "call_correct"]

# Calls and which outcomes count as correct
CALLS = {
    "HOME_WIN":     {"H"},
    "HOME_NOT_LOSE": {"H", "D"},
    "HOME_NOT_WIN": {"D", "A"},
    "NO_CALL":      set(),
}

# ── Index (same calculation as dashboard_v4) ──
SCORED = [("sotAtk",1,1),("sotDef",0,1),("xgShD",1,1),("xgShAtk",1,1),("shotAcc",1,1),("shotsInBox",1,1),
          ("fb90",1,2),("da90",1,2),("bu90",1,2),("tball90",1,2),("crossAcc",1,2),("spXga",1,3),("ppda",0,3)]

def mapteam(t):
    gp = t["atk_played"]; per = lambda v: round(v / gp, 3)
    m = dict(sotAtk=per(t["atk_shots_ot"]), sotDef=per(t["def_sot_faced"]),
             xgShAtk=t["np_xg_per_shot"], xgShDef=t["np_xga_per_shot"],
             shotAcc=round(t["atk_shots_ot"] / t["atk_shots"] * 100, 1) if t["atk_shots"] else None,
             shotsInBox=t["atk_shots_in_box_perc"], fb90=per(t["atk_fast_break_shots"]),
             da90=per(t["seq_direct_attacks"]), bu90=per(t["seq_buildups"]),
             tball90=per(t["pass_through_balls"]),
             spXga=round((t["sp_atk_xg"] - t["sp_def_xga"]) / gp, 3),
             ppda=t["press_ppda"], crossAcc=t["pass_cross_acc"])
    m["xgShD"] = round(m["xgShAtk"] - m["xgShDef"], 3)
    return m

def score(rows):
    pct = {c: {} for c in rows}
    for k, hi, _ in SCORED:
        vals = sorted(rows[c][k] for c in rows if rows[c][k] is not None)
        for c in rows:
            v = rows[c][k]
            if v is None: continue
            p = vals.index(v) / (len(vals) - 1) * 100
            pct[c][k] = p if hi else 100 - p
    out = {}
    for c in rows:
        tiers = []
        for n in (1, 2, 3):
            ks = [k for k, _, t in SCORED if t == n and k in pct[c]]
            tiers.append(sum(pct[c][k] for k in ks) / len(ks))
        out[c] = round(tiers[0] * .5 + tiers[1] * .3 + tiers[2] * .2, 1)
    return out

# ── Rules ──
def make_call(gap, appg):
    if appg is None or abs(gap) < 10:
        return "NO_CALL", "none"
    if gap <= -10 and appg >= 1.0:
        return "HOME_NOT_WIN", "strong" if appg >= 1.2 else "medium"
    if gap >= 10 and appg < 1.0:
        return "HOME_WIN", "medium"
    if gap >= 10:
        return "HOME_NOT_LOSE", "medium"
    return "NO_CALL", "none"   # home 10+ behind but visitors poor away: no edge in backtest

# ── Helpers ──
def load():
    if not os.path.exists(CSV): return []
    with open(CSV) as f: return list(csv.DictReader(f))

def save(rows):
    with open(CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLS); w.writeheader(); w.writerows(rows)

def find(names, key):
    k = key.lower().replace("utd", "united")
    hits = [n for n in names if k in n.lower()]
    if len(hits) != 1:
        hits = [n for n in names if n.lower().startswith(k)]
    if len(hits) != 1: sys.exit(f"Can't match team '{key}': {hits}")
    return hits[0]

def market(oh, od, oa):
    p = [1 / oh, 1 / od, 1 / oa]; s = sum(p)
    return [x / s for x in p]

def cmd_call(date, fixtures):
    snap = json.load(open(HIST))[-1]
    idx = score({n: mapteam(t) for n, t in snap["teams"].items()})
    away_tbl = {r["name"]: r for r in snap["standings"]["away"]}
    rows = load()
    for fx in fixtures:
        parts = fx.split("|")
        h, a = find(idx, parts[0]), find(idx, parts[1])
        if any(r["date"] == date and r["home"] == h and r["away"] == a for r in rows):
            print(f"skip (already logged): {h} v {a}"); continue
        aw = away_tbl.get(a, {})
        appg = aw["points"] / aw["played"] if aw.get("played") else None
        gap = round(idx[h] - idx[a], 1)
        call, conf = make_call(gap, appg)
        row = {c: "" for c in COLS}
        row.update(date=date, home=h, away=a, snapshot=snap["date"], home_idx=idx[h], away_idx=idx[a],
                   gap=gap, away_side_away_ppg=round(appg, 2) if appg is not None else "",
                   call=call, confidence=conf)
        if len(parts) >= 5:
            o = [float(x) for x in parts[2:5]]
            m = market(*o)
            row.update(odds_h=o[0], odds_d=o[1], odds_a=o[2],
                       mkt_h=round(m[0], 3), mkt_d=round(m[1], 3), mkt_a=round(m[2], 3))
            if CALLS[call]:
                row["mkt_prob_of_call"] = round(sum(m[i] for i, k in enumerate("HDA") if k in CALLS[call]), 3)
        rows.append(row)
        print(f"{h} v {a}: gap {gap:+}, call {call} ({conf})")
    save(rows)

def cmd_result(date, results):
    rows = load()
    for fx in results:
        h_, a_, hg, ag = fx.split("|")
        hit = [r for r in rows if r["date"] == date and h_.lower().replace("utd","united") in r["home"].lower()
               and a_.lower().replace("utd","united") in r["away"].lower()]
        if len(hit) != 1: print(f"no unique row for {fx}"); continue
        r = hit[0]; hg, ag = int(hg), int(ag)
        out = "H" if hg > ag else "A" if ag > hg else "D"
        r.update(hg=hg, ag=ag, outcome=out)
        r["call_correct"] = "" if r["call"] == "NO_CALL" else int(out in CALLS[r["call"]])
        print(f"{r['home']} {hg}-{ag} {r['away']}: {r['call']} -> {r['call_correct']}")
    save(rows)

def cmd_summary():
    rows = [r for r in load() if r["outcome"]]
    called = [r for r in rows if r["call"] != "NO_CALL"]
    lines = ["# xB Index prediction tracker", "",
             f"Matches scored: {len(rows)}. Index calls made: {len(called)}.", ""]
    def block(label, rs):
        if not rs: return
        hits = sum(int(r["call_correct"]) for r in rs)
        priced = [r for r in rs if r["mkt_prob_of_call"]]
        mkt = sum(float(r["mkt_prob_of_call"]) for r in priced) / len(priced) if priced else None
        hit_p = sum(int(r["call_correct"]) for r in priced) / len(priced) if priced else None
        s = f"| {label} | {hits}/{len(rs)} ({hits/len(rs):.0%}) |"
        s += f" {hit_p:.0%} vs {mkt:.0%} ({len(priced)} priced) |" if mkt is not None else " no odds |"
        lines.append(s)
    lines += ["| Calls | Correct | Index hit rate vs bookies' expected rate |", "|---|---|---|"]
    block("All calls", called)
    for c in ("HOME_NOT_WIN", "HOME_WIN", "HOME_NOT_LOSE"):
        block(c.replace("_", " ").title(), [r for r in called if r["call"] == c])
    for conf in ("strong", "medium"):
        block(f"Confidence: {conf}", [r for r in called if r["confidence"] == conf])
    lines += ["", "If the Index hit rate stays above the bookies' expected rate over a few months, "
              "the Index is seeing something the market isn't. One weekend proves nothing.", ""]
    open(SUMMARY, "w").write("\n".join(lines))
    print("\n".join(lines))

if __name__ == "__main__":
    a = sys.argv[1:]
    if not a: sys.exit(__doc__)
    {"call": lambda: cmd_call(a[1], a[2:]), "result": lambda: cmd_result(a[1], a[2:]),
     "summary": cmd_summary}[a[0]]()
