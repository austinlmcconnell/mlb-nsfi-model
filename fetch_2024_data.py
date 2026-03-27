"""
Fetch 2024 MLB season data from the MLB Stats API to update all model CSV files.

Files updated:
  - pitching_vs_left_year.csv / pitching_vs_left_season.csv
  - pitching_vs_right_year.csv / pitching_vs_right_season.csv
  - batting_vs_left_year.csv / batting_vs_left_season.csv
  - batting_vs_right_year.csv / batting_vs_right_season.csv
  - MLB_season.csv
  - RHP_data.csv / LHP_data.csv
  - RHH_data.csv / LHH_data.csv / switch_data.csv
"""

import requests
import pandas as pd
import time

BASE = "https://statsapi.mlb.com/api/v1"
SEASON = 2025

# Team ID → abbreviation mapping
TEAM_ABBREV = {
    108: "LAA", 109: "ARI", 110: "BAL", 111: "BOS", 112: "CHC",
    113: "CIN", 114: "CLE", 115: "COL", 116: "DET", 117: "HOU",
    118: "KCR", 119: "LAD", 120: "WSN", 121: "NYM", 133: "OAK",
    134: "PIT", 135: "SDP", 136: "SEA", 137: "SFG", 138: "STL",
    139: "TBR", 140: "TEX", 141: "TOR", 142: "MIN", 143: "PHI",
    144: "ATL", 145: "CWS", 146: "MIA", 147: "NYY", 158: "MIL",
}


def fetch(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise


def aggregate_by_player(rows, count_col):
    """
    Aggregate split rows for players who appeared on multiple teams.
    Weighted-average rate stats by TBF/PA; sum counting stats.
    Returns one row per player name.
    """
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Numeric counting columns to sum
    int_cols = [c for c in df.columns if c not in ("Season", "Name", "Tm", "ERA", "AVG", "OBP", "SLG", "wOBA", "playerId")]

    def agg_player(grp):
        if len(grp) == 1:
            row = grp.iloc[0].copy()
            row["Tm"] = grp["Tm"].iloc[0]
            return row
        # Multiple teams — sum counting stats, recalculate rates
        out = grp.iloc[0].copy()
        out["Tm"] = "Multi"
        for c in int_cols:
            try:
                out[c] = grp[c].astype(float).sum()
            except Exception:
                pass
        # Recalculate AVG, OBP, SLG from summed counting stats
        tbf_or_pa = out.get(count_col, 1) or 1
        h = out.get("H", 0)
        bb = out.get("BB", 0)
        ibb = out.get("IBB", 0)
        hbp = out.get("HBP", 0)
        ab = out.get("AB", tbf_or_pa - bb - ibb - hbp)
        doubles = out.get("2B", 0)
        triples = out.get("3B", 0)
        hr = out.get("HR", 0)
        singles = h - doubles - triples - hr
        out["AVG"] = f".{int(h/ab*1000):03d}" if ab > 0 else ".000"
        obp_num = h + bb + hbp
        obp_den = ab + bb + hbp + out.get("SF", 0)
        out["OBP"] = f".{int(obp_num/obp_den*1000):03d}" if obp_den > 0 else ".000"
        tb = singles + 2*doubles + 3*triples + 4*hr
        out["SLG"] = f".{int(tb/ab*1000):03d}" if ab > 0 else ".000"
        return out

    # Group by (Name, playerId) to aggregate multi-team rows
    agg = df.groupby(["Name", "playerId"], as_index=False, sort=False).apply(agg_player)
    return agg.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 1. Pitcher splits (vs LHB and vs RHB)
# ---------------------------------------------------------------------------

def fetch_pitcher_splits(sit_code):
    data = fetch(f"{BASE}/stats", params={
        "stats": "statSplits",
        "group": "pitching",
        "season": SEASON,
        "gameType": "R",
        "sportId": 1,
        "limit": 5000,
        "offset": 0,
        "playerPool": "All",
        "sitCodes": sit_code,
    })
    splits = data["stats"][0]["splits"]
    rows = []
    for s in splits:
        st = s["stat"]
        rows.append({
            "Season": "Total",
            "Name": s["player"]["fullName"],
            "Tm": TEAM_ABBREV.get(s.get("team", {}).get("id", 0), ""),
            "G": int(st.get("gamesPlayed", 0)),
            "TBF": int(st.get("battersFaced", 0)),
            "ERA": st.get("era", "0.00"),
            "H": int(st.get("hits", 0)),
            "2B": int(st.get("doubles", 0)),
            "3B": int(st.get("triples", 0)),
            "R": int(st.get("runs", 0)),
            "ER": int(st.get("earnedRuns", 0)),
            "HR": int(st.get("homeRuns", 0)),
            "BB": int(st.get("baseOnBalls", 0)),
            "IBB": int(st.get("intentionalWalks", 0)),
            "HBP": int(st.get("hitBatsmen", 0)),
            "SO": int(st.get("strikeOuts", 0)),
            "AVG": st.get("avg", ".000"),
            "OBP": st.get("obp", ".000"),
            "SLG": st.get("slg", ".000"),
            "wOBA": "",
            "playerId": s["player"]["id"],
        })
    df = aggregate_by_player(rows, count_col="TBF")
    return df


print("Fetching pitcher splits vs LHB...")
pitch_vs_left = fetch_pitcher_splits("vl")
print(f"  → {len(pitch_vs_left)} unique pitchers")

print("Fetching pitcher splits vs RHB...")
pitch_vs_right = fetch_pitcher_splits("vr")
print(f"  → {len(pitch_vs_right)} unique pitchers")


# ---------------------------------------------------------------------------
# 2. Batter splits (vs LHP and vs RHP)
# ---------------------------------------------------------------------------

def fetch_batter_splits(sit_code):
    data = fetch(f"{BASE}/stats", params={
        "stats": "statSplits",
        "group": "hitting",
        "season": SEASON,
        "gameType": "R",
        "sportId": 1,
        "limit": 5000,
        "offset": 0,
        "playerPool": "All",
        "sitCodes": sit_code,
    })
    splits = data["stats"][0]["splits"]
    rows = []
    for s in splits:
        st = s["stat"]
        hits = int(st.get("hits", 0))
        doubles = int(st.get("doubles", 0))
        triples = int(st.get("triples", 0))
        hr = int(st.get("homeRuns", 0))
        singles = max(hits - doubles - triples - hr, 0)
        rows.append({
            "Season": "Total",
            "Name": s["player"]["fullName"],
            "Tm": TEAM_ABBREV.get(s.get("team", {}).get("id", 0), ""),
            "G": int(st.get("gamesPlayed", 0)),
            "PA": int(st.get("plateAppearances", 0)),
            "AB": int(st.get("atBats", 0)),
            "H": hits,
            "1B": singles,
            "2B": doubles,
            "3B": triples,
            "HR": hr,
            "R": int(st.get("runs", 0)),
            "RBI": int(st.get("rbi", 0)),
            "BB": int(st.get("baseOnBalls", 0)),
            "IBB": int(st.get("intentionalWalks", 0)),
            "SO": int(st.get("strikeOuts", 0)),
            "HBP": int(st.get("hitByPitch", 0)),
            "SF": int(st.get("sacFlies", 0)),
            "SH": int(st.get("sacBunts", 0)),
            "GDP": int(st.get("groundIntoDoublePlay", 0)),
            "SB": int(st.get("stolenBases", 0)),
            "CS": int(st.get("caughtStealing", 0)),
            "AVG": st.get("avg", ".000"),
            "playerId": s["player"]["id"],
        })
    df = aggregate_by_player(rows, count_col="PA")
    return df


print("Fetching batter splits vs LHP...")
bat_vs_left = fetch_batter_splits("vl")
print(f"  → {len(bat_vs_left)} unique batters")

print("Fetching batter splits vs RHP...")
bat_vs_right = fetch_batter_splits("vr")
print(f"  → {len(bat_vs_right)} unique batters")


# ---------------------------------------------------------------------------
# 3. MLB season averages (2024 league-wide totals)
# ---------------------------------------------------------------------------

print("Fetching MLB 2024 league-wide averages...")
data = fetch(f"{BASE}/stats", params={
    "stats": "season",
    "group": "pitching",
    "season": SEASON,
    "gameType": "R",
    "sportId": 1,
    "limit": 5000,
    "playerPool": "All",
})
splits = data["stats"][0]["splits"]

totals = {k: 0 for k in ["TBF", "H", "2B", "3B", "R", "ER", "HR", "BB", "IBB", "HBP", "SO", "G", "IP_outs"]}
seen = set()
for s in splits:
    pid = s["player"]["id"]
    if pid in seen:   # skip duplicates
        continue
    seen.add(pid)
    st = s["stat"]
    totals["TBF"] += int(st.get("battersFaced", 0))
    totals["H"] += int(st.get("hits", 0))
    totals["2B"] += int(st.get("doubles", 0))
    totals["3B"] += int(st.get("triples", 0))
    totals["R"] += int(st.get("runs", 0))
    totals["ER"] += int(st.get("earnedRuns", 0))
    totals["HR"] += int(st.get("homeRuns", 0))
    totals["BB"] += int(st.get("baseOnBalls", 0))
    totals["IBB"] += int(st.get("intentionalWalks", 0))
    totals["HBP"] += int(st.get("hitBatsmen", 0))
    totals["SO"] += int(st.get("strikeOuts", 0))
    totals["G"] += int(st.get("gamesPlayed", 0))
    ip_str = str(st.get("inningsPitched", "0.0"))
    try:
        parts = ip_str.split(".")
        totals["IP_outs"] += int(parts[0]) * 3 + (int(parts[1]) if len(parts) == 2 else 0)
    except Exception:
        pass

ip_innings = totals["IP_outs"] / 3 if totals["IP_outs"] > 0 else 1
era = totals["ER"] * 9 / ip_innings
avg = totals["H"] / totals["TBF"] if totals["TBF"] > 0 else 0
obp_num = totals["H"] + totals["BB"] + totals["HBP"]
obp_den = totals["TBF"] - totals["IBB"]
obp = obp_num / obp_den if obp_den > 0 else 0
ab_est = totals["TBF"] - totals["BB"] - totals["HBP"] - totals["IBB"]
singles_lg = totals["H"] - totals["2B"] - totals["3B"] - totals["HR"]
tb = singles_lg + 2*totals["2B"] + 3*totals["3B"] + 4*totals["HR"]
slg = tb / ab_est if ab_est > 0 else 0

mlb_season = pd.DataFrame([{
    "Season": "Total",
    "League": "MLB",
    "G": totals["G"],
    "TBF": totals["TBF"],
    "ERA": round(era, 5),
    "H": totals["H"],
    "2B": totals["2B"],
    "3B": totals["3B"],
    "R": totals["R"],
    "ER": totals["ER"],
    "HR": totals["HR"],
    "BB": totals["BB"],
    "IBB": totals["IBB"],
    "HBP": totals["HBP"],
    "SO": totals["SO"],
    "AVG": round(avg, 9),
    "OBP": round(obp, 9),
    "SLG": round(slg, 9),
    "wOBA": "",
}])
k_rate = totals["SO"] / totals["TBF"] if totals["TBF"] > 0 else 0
bb_rate = totals["BB"] / totals["TBF"] if totals["TBF"] > 0 else 0
print(f"  K%={k_rate:.4f}, BB%={bb_rate:.4f}, HR%={(totals['HR']/totals['TBF']):.4f}")


# ---------------------------------------------------------------------------
# 4. Player handedness
# ---------------------------------------------------------------------------

print("Fetching 2024 player handedness...")
data = fetch(f"{BASE}/sports/1/players", params={"season": SEASON})
people = data.get("people", [])

pitchers_r, pitchers_l = [], []
batters_r, batters_l, batters_s = [], [], []

for p in people:
    name = p.get("fullName", "")
    team = p.get("currentTeam", {}).get("name", "")
    mlbam_id = p.get("id", "")
    position_type = p.get("primaryPosition", {}).get("type", "")
    bat_side = p.get("batSide", {}).get("code", "")
    pitch_hand = p.get("pitchHand", {}).get("code", "")

    if position_type == "Pitcher":
        if pitch_hand == "R":
            pitchers_r.append({"Name": name, "Team": team, "MLBAMID": mlbam_id})
        elif pitch_hand == "L":
            pitchers_l.append({"Name": name, "Team": team, "MLBAMID": mlbam_id})
    else:
        if bat_side == "R":
            batters_r.append({"Name": name, "Team": team, "MLBAMID": mlbam_id})
        elif bat_side == "L":
            batters_l.append({"Name": name, "Team": team, "MLBAMID": mlbam_id})
        elif bat_side == "S":
            batters_s.append({"Name": name, "Team": team, "MLBAMID": mlbam_id})

RHP_df = pd.DataFrame(pitchers_r)
LHP_df = pd.DataFrame(pitchers_l)
RHH_df = pd.DataFrame(batters_r)
LHH_df = pd.DataFrame(batters_l)
switch_df = pd.DataFrame(batters_s)

print(f"  RHP: {len(RHP_df)}, LHP: {len(LHP_df)}, RHH: {len(RHH_df)}, LHH: {len(LHH_df)}, Switch: {len(switch_df)}")


# ---------------------------------------------------------------------------
# 5. Write all CSV files
# ---------------------------------------------------------------------------

print("\nWriting CSV files...")

pitch_vs_left.to_csv("pitching_vs_left_year.csv", index=False)
pitch_vs_left.to_csv("pitching_vs_left_season.csv", index=False)
pitch_vs_right.to_csv("pitching_vs_right_year.csv", index=False)
pitch_vs_right.to_csv("pitching_vs_right_season.csv", index=False)
print("  ✓ Pitching splits (4 files)")

bat_vs_left.to_csv("batting_vs_left_year.csv", index=False)
bat_vs_left.to_csv("batting_vs_left_season.csv", index=False)
bat_vs_right.to_csv("batting_vs_right_year.csv", index=False)
bat_vs_right.to_csv("batting_vs_right_season.csv", index=False)
print("  ✓ Batting splits (4 files)")

mlb_season.to_csv("MLB_season.csv", index=False)
print("  ✓ MLB_season.csv")

RHP_df.to_csv("RHP_data.csv", index=False)
LHP_df.to_csv("LHP_data.csv", index=False)
RHH_df.to_csv("RHH_data.csv", index=False)
LHH_df.to_csv("LHH_data.csv", index=False)
switch_df.to_csv("switch_data.csv", index=False)
print("  ✓ Handedness files (5 files)")

print("\nAll 14 CSV files updated successfully.\n")

# ---------------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------------

print("=== Sanity Checks ===")
p_left = pd.read_csv("pitching_vs_left_year.csv")
p_right = pd.read_csv("pitching_vs_right_year.csv")
b_left = pd.read_csv("batting_vs_left_year.csv")
b_right = pd.read_csv("batting_vs_right_year.csv")
mlb = pd.read_csv("MLB_season.csv")

print(f"\nPitcher splits: {len(p_left)} vs LHB, {len(p_right)} vs RHB")
print(f"Batter splits:  {len(b_left)} vs LHP, {len(b_right)} vs RHP")
print(f"Duplicate pitcher names: {p_left['Name'].duplicated().sum()}")
print(f"Duplicate batter names:  {b_left['Name'].duplicated().sum()}")

# Spot check top starters
starters = ["Zack Wheeler", "Corbin Burnes", "Logan Webb", "Paul Skenes", "Chris Sale", "Dylan Cease"]
found = p_left[p_left["Name"].isin(starters)][["Name", "Tm", "G", "TBF", "SO", "BB", "HR"]]
print(f"\nTop starters in pitcher vs LHB data:\n{found.to_string()}")

# MLB rates
mlb["K_Rate"] = mlb["SO"] / mlb["TBF"]
mlb["BB_Rate"] = mlb["BB"] / mlb["TBF"]
mlb["1B_Rate"] = (mlb["H"] - mlb["2B"] - mlb["3B"] - mlb["HR"]) / mlb["TBF"]
mlb["HR_Rate"] = mlb["HR"] / mlb["TBF"]
print(f"\n2024 MLB rates (league avg fallback):")
print(mlb[["K_Rate", "BB_Rate", "1B_Rate", "HR_Rate"]].to_string())

# Handedness check
rhp = pd.read_csv("RHP_data.csv")
lhp = pd.read_csv("LHP_data.csv")
sw = pd.read_csv("switch_data.csv")
print(f"\nHandedness: {len(rhp)} RHP, {len(lhp)} LHP, {len(sw)} switch hitters")
known_pitchers = ["Zack Wheeler", "Logan Webb", "Paul Skenes", "Chris Sale"]
print("Known pitchers in RHP list:", [n for n in known_pitchers if n in rhp["Name"].values])
known_lhp = ["Tarik Skubal", "Chris Sale", "Framber Valdez"]
print("Known LHPs in LHP list:", [n for n in known_lhp if n in lhp["Name"].values])
