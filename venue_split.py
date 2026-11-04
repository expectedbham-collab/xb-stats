#!/usr/bin/env python3
"""
xB venue split builder
----------------------
Builds home-only and away-only stat totals for every club, for every
snapshot of the current season, and writes them to data/venue.json.

How it works
  Opta's tournament stats are season totals with no venue split. So each
  club's stats are differenced between consecutive snapshots: the change
  between two snapshots is the club's output in the match (or matches)
  played in between. Each match is then tagged home or away using the
  standings, which carry separate home and away played counts.

  If a gap between two snapshots covers both a home and an away match for
  a club, the change can't be split. That block is left out rather than
  guessed, and the dashboard shows how many matches each figure rests on.

  data/fixtures_seed.json (optional) fixes the home/away order for blocks
  the standings can't resolve (two matches between snapshots, one home and
  one away). It is only ever used to order matches, never for stats.

Usage
  python venue_split.py
"""

import json, os
from datetime import date

ROOT = os.path.dirname(os.path.abspath(__file__))
HISTORY_PATH = os.path.join(ROOT, "data", "history.json")
SEED_PATH    = os.path.join(ROOT, "data", "fixtures_seed.json")
VENUE_PATH   = os.path.join(ROOT, "data", "venue.json")

PEN_XG = 0.79  # only used for early snapshots that lack non-penalty fields

COUNT_KEYS = ["shots_ot", "sot_faced", "np_xg", "np_shots", "np_xga", "np_sa",
              "shots", "shots_in_box", "fast_breaks", "direct_attacks",
              "buildups", "through_balls", "sp_xg", "sp_xga", "crosses",
              "crosses_ok", "ppda_x_gp"]


def counts(t):
    """Turn one club's season-total snapshot into additive counts."""
    np_xg, np_shots = t.get("np_xg"), t.get("np_shots")
    np_xga, np_sa = t.get("np_xga"), t.get("np_shots_against")
    if np_xga is None or np_xg is None:
        pf, pa = t.get("atk_pens", 0) or 0, t.get("misc_pens_conceded", 0) or 0
        np_xg, np_shots = t["atk_xg"] - PEN_XG * pf, t["atk_shots"] - pf
        np_xga, np_sa = t["def_xga"] - PEN_XG * pa, t["def_shots_faced"] - pa
    return {
        "shots_ot": t["atk_shots_ot"], "sot_faced": t["def_sot_faced"],
        "np_xg": np_xg, "np_shots": np_shots, "np_xga": np_xga, "np_sa": np_sa,
        "shots": t["atk_shots"],
        "shots_in_box": t["atk_shots_in_box_perc"] * t["atk_shots"] / 100,
        "fast_breaks": t["atk_fast_break_shots"], "direct_attacks": t["seq_direct_attacks"],
        "buildups": t["seq_buildups"], "through_balls": t["pass_through_balls"],
        "sp_xg": t["sp_atk_xg"], "sp_xga": t["sp_def_xga"],
        "crosses": t["pass_crosses"], "crosses_ok": t["pass_crosses_success"],
        # PPDA isn't additive; weight it by games so it can be split approximately
        "ppda_x_gp": t["press_ppda"] * t["atk_played"],
    }


def current_season(history):
    snaps = sorted(history, key=lambda s: s["date"])
    latest_teams = set(snaps[-1]["teams"])
    season = [s for s in snaps if set(s["teams"]) == latest_teams]
    # drop anything from an earlier season with the same 24 clubs:
    # played counts must never go down through the season
    out = []
    for s in season:
        mx = max(t["atk_played"] for t in s["teams"].values())
        if out and mx < max(t["atk_played"] for t in out[-1]["teams"].values()):
            out = []
        out.append(s)
    return out


def venue_sequences(season, seed):
    """Per club: list of 'H'/'A'/'?' by match number, from standings increments."""
    seq = {c: [] for c in season[-1]["teams"]}
    prev = {c: (0, 0) for c in seq}
    for s in season:
        st = s.get("standings") or {}
        hp = {r["name"]: r["played"] for r in st.get("home", [])}
        ap = {r["name"]: r["played"] for r in st.get("away", [])}
        for c in seq:
            if c not in hp or c not in ap:
                continue
            # early-season snapshots can carry last season's standings: skip them
            stats_played = s["teams"].get(c, {}).get("atk_played", 0)
            if not (stats_played <= hp[c] + ap[c] <= stats_played + 5):
                continue
            h0, a0 = prev[c]
            dh, da = hp[c] - h0, ap[c] - a0
            if dh < 0 or da < 0:
                continue
            if dh and da:
                seq[c] += ["?"] * (dh + da)
            else:
                seq[c] += ["H"] * dh + ["A"] * da
            prev[c] = (hp[c], ap[c])
    # the seed fixes order where the standings can't
    for c, s_seq in seed.items():
        for i, v in enumerate(s_seq):
            if i < len(seq.get(c, [])):
                seq[c][i] = v
            elif c in seq:
                seq[c].append(v)
    return seq


def load_seed():
    if not os.path.exists(SEED_PATH):
        return {}
    fx = json.load(open(SEED_PATH))
    fx.sort(key=lambda m: m["date"])
    seq = {}
    for m in fx:
        seq.setdefault(m["home"], []).append("H")
        seq.setdefault(m["away"], []).append("A")
    return seq


def build():
    history = json.load(open(HISTORY_PATH))
    season = current_season(history)
    seq = venue_sequences(season, load_seed())

    clubs = list(season[-1]["teams"])
    blank = lambda: dict({k: 0.0 for k in COUNT_KEYS}, gp=0)
    cur = {c: {"home": blank(), "away": blank()} for c in clubs}
    prev = {c: (0, {k: 0.0 for k in COUNT_KEYS}) for c in clubs}
    out = []
    for s in season:
        st = s.get("standings") or {}
        hp = {r["name"]: r["played"] for r in st.get("home", [])}
        ap = {r["name"]: r["played"] for r in st.get("away", [])}
        for c, t in s["teams"].items():
            p, pc = prev[c]
            q = t["atk_played"]
            if q <= p:
                continue
            nc = counts(t)
            venues = set(seq[c][p:q]) if len(seq[c]) >= q else {"?"}
            if len(venues) == 1 and "?" not in venues:
                b = cur[c]["home" if venues.pop() == "H" else "away"]
                for k in COUNT_KEYS:
                    b[k] += nc[k] - pc[k]
                b["gp"] += q - p
            prev[c] = (q, nc)
        snap_teams = {}
        for c in clubs:
            snap_teams[c] = {}
            for v, played in (("home", hp), ("away", ap)):
                b = {k: round(val, 4) for k, val in cur[c][v].items()}
                b["played"] = played.get(c)
                snap_teams[c][v] = b
        out.append({"date": s["date"], "label": s.get("label"), "teams": snap_teams})

    payload = {"generated": date.today().isoformat(), "snapshots": out}
    with open(VENUE_PATH, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    last = out[-1]["teams"]
    tracked = sum(last[c][v]["gp"] for c in clubs for v in ("home", "away"))
    played = sum(last[c][v]["played"] or 0 for c in clubs for v in ("home", "away"))
    print(f"venue.json written: {len(out)} snapshots, {int(tracked)} of {played} club-matches split by venue")


if __name__ == "__main__":
    build()
