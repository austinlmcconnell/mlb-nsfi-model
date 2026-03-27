import streamlit as st
import pandas as pd
import numpy as np
import requests
import json
import time
from datetime import datetime, timezone, timedelta

st.set_page_config(page_title="MLB NSFI Dashboard", layout="wide", page_icon="\u26be")

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #ffffff;
        margin-bottom: 0;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #888;
        margin-top: -10px;
    }
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #2a2a4a;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
    .bet-take {
        background: linear-gradient(135deg, #0d3320 0%, #1a4a2e 100%);
        border: 1px solid #2d6b45;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 10px;
    }
    .bet-skip {
        background: linear-gradient(135deg, #3a1a1a 0%, #4a2020 100%);
        border: 1px solid #6b2d2d;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 10px;
    }
    .bet-marginal {
        background: linear-gradient(135deg, #3a3a1a 0%, #4a4a20 100%);
        border: 1px solid #6b6b2d;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 10px;
    }
    .ev-positive { color: #4ade80; font-weight: 700; }
    .ev-negative { color: #f87171; font-weight: 700; }
    .ev-marginal { color: #fbbf24; font-weight: 700; }
    div[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f1419 0%, #1a1f2e 100%);
    }
    .stDataFrame { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-header">\u26be MLB NSFI Betting Dashboard</p>', unsafe_allow_html=True)
st.markdown(f'<p class="sub-header">DraftKings 1st Inning Strikeout Markets \u2022 {datetime.now().strftime("%B %d, %Y")}</p>', unsafe_allow_html=True)

# ── DraftKings odds fetcher ───────────────────────────────────────────────────

TEAM_TO_ABBREV = {
    "Los Angeles Angels": "LAA", "Arizona Diamondbacks": "ARI",
    "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL", "Detroit Tigers": "DET",
    "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Dodgers": "LAD", "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL", "Minnesota Twins": "MIN",
    "New York Mets": "NYM", "New York Yankees": "NYY",
    "Oakland Athletics": "OAK", "Sacramento Athletics": "OAK",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB", "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH",
    "Atlanta Braves": "ATL",
}

def american_to_implied(odds: int) -> float:
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)

def implied_to_american(prob: float) -> str:
    if prob <= 0 or prob >= 1:
        return "N/A"
    if prob >= 0.5:
        return f"{round(-(prob / (1 - prob)) * 100):+d}"
    return f"+{round(((1 - prob) / prob) * 100)}"

@st.cache_data(ttl=300)
def fetch_dk_odds(date_str: str):
    """
    Load DraftKings NSFI odds. Strategy:
      1. Read from daily_YYYYMMDD.json in the repo (pre-fetched locally, geo-safe)
      2. Fall back to direct DraftKings API (works on local machines, blocked on cloud)
    Returns (dict keyed by team name, source_label, error_str).
    """
    import os

    # ── 1. Try daily JSON ─────────────────────────────────────────────────────
    json_path = os.path.join(os.path.dirname(__file__),
                             f"daily_{date_str.replace('-', '')}.json")
    if os.path.exists(json_path):
        try:
            with open(json_path) as f:
                daily = json.load(f)
            result = {}
            for game in daily.get("games", []):
                dk = (game.get("odds") or {}).get("draftkings") or {}
                home = game.get("homeTeam", "")
                away = game.get("awayTeam", "")
                # top slot = home team pitching, bot slot = away team pitching
                for slot, team in [("top", home), ("bot", away)]:
                    entry = dk.get(slot, {})
                    if not entry.get("oddsPosted"):
                        continue
                    no_odds = entry.get("noOdds")
                    if no_odds is None:
                        continue
                    result[team] = {
                        "noOdds": no_odds,
                        "yesOdds": entry.get("yesOdds"),
                        "impliedNSFI": round(american_to_implied(no_odds), 4),
                    }
            if result:
                fetched_at = daily.get("fetchedAt", "")
                return result, f"daily JSON ({fetched_at[:16].replace('T', ' ')} UTC)", None
        except Exception as e:
            pass

    return None, None, (
        f"No daily data found. Run `python fetch_daily.py` locally "
        f"and push `daily_{date_str.replace('-','')}.json` to the repo."
    )


# ── MLB lineup fetcher ────────────────────────────────────────────────────────

MLB_BASE = "https://statsapi.mlb.com/api/v1"

TEAM_TO_BALLPARK = {
    "Los Angeles Angels": "Angels", "Arizona Diamondbacks": "Diamondbacks",
    "Baltimore Orioles": "Orioles", "Boston Red Sox": "Red Sox",
    "Chicago Cubs": "Cubs", "Chicago White Sox": "White Sox",
    "Cincinnati Reds": "Reds", "Cleveland Guardians": "Guardians",
    "Colorado Rockies": "Rockies", "Detroit Tigers": "Tigers",
    "Houston Astros": "Astros", "Kansas City Royals": "Royals",
    "Los Angeles Dodgers": "Dodgers", "Miami Marlins": "Marlins",
    "Milwaukee Brewers": "Brewers", "Minnesota Twins": "Twins",
    "New York Mets": "Mets", "New York Yankees": "Yankees",
    "Oakland Athletics": "Athletics", "Sacramento Athletics": "Athletics",
    "Philadelphia Phillies": "Phillies", "Pittsburgh Pirates": "Pirates",
    "San Diego Padres": "Padres", "San Francisco Giants": "Giants",
    "Seattle Mariners": "Mariners", "St. Louis Cardinals": "Cardinals",
    "Tampa Bay Rays": "Rays", "Texas Rangers": "Rangers",
    "Toronto Blue Jays": "Blue Jays", "Washington Nationals": "Nationals",
    "Atlanta Braves": "Braves",
}

@st.cache_data(ttl=300)
def fetch_today_games(date_str):
    """Fetch today's MLB schedule with lineups and probable pitchers."""
    try:
        r = requests.get(MLB_BASE + "/schedule", params={
            "sportId": 1, "date": date_str,
            "hydrate": "lineups,probablePitcher,team,venue",
            "gameType": "R",
        }, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return None, str(e)

    dates = data.get("dates", [])
    if not dates:
        return [], None
    raw_games = dates[0].get("games", [])

    # Collect all player IDs to resolve handedness in one batch
    all_ids = []
    for g in raw_games:
        lineups = g.get("lineups", {})
        for p in lineups.get("homePlayers", []) + lineups.get("awayPlayers", []):
            all_ids.append(p["id"])
        for side in ["home", "away"]:
            prob = g["teams"][side].get("probablePitcher")
            if prob:
                all_ids.append(prob["id"])

    # Batch resolve handedness
    handedness = {}
    for i in range(0, len(all_ids), 150):
        chunk = all_ids[i:i+150]
        try:
            r2 = requests.get(MLB_BASE + "/people",
                               params={"personIds": ",".join(str(x) for x in chunk)},
                               timeout=20)
            r2.raise_for_status()
            for p in r2.json().get("people", []):
                handedness[p["id"]] = {
                    "batSide": p.get("batSide", {}).get("code", "R"),
                    "pitchHand": p.get("pitchHand", {}).get("code", "R"),
                    "fullName": p.get("fullName", ""),
                }
        except Exception:
            pass

    abbrev = lambda name: TEAM_TO_ABBREV.get(name, name[:3].upper())

    games = []
    for g in raw_games:
        home = g["teams"]["home"]["team"]["name"]
        away = g["teams"]["away"]["team"]["name"]
        game_time_utc = g.get("gameDate", "")
        try:
            dt_utc = datetime.fromisoformat(game_time_utc.replace("Z", "+00:00"))
            dt_et = dt_utc - timedelta(hours=4)
            game_time_et = dt_et.strftime("%-I:%M %p ET")
        except Exception:
            game_time_et = game_time_utc

        lineups = g.get("lineups", {})
        home_players = lineups.get("homePlayers", [])
        away_players = lineups.get("awayPlayers", [])
        lineup_complete = (len(home_players) == 9 and len(away_players) == 9)

        def build_pitcher(prob):
            if not prob:
                return {"name": "TBD", "id": None, "pitchHand": "R"}
            h = handedness.get(prob["id"], {})
            return {"name": prob["fullName"], "id": prob["id"],
                    "pitchHand": h.get("pitchHand", "R")}

        def build_lineup(players):
            return [{"name": p["fullName"], "id": p["id"],
                     "batSide": handedness.get(p["id"], {}).get("batSide", "R"),
                     "position": p.get("primaryPosition", {}).get("abbreviation", "")}
                    for p in players]

        home_prob = g["teams"]["home"].get("probablePitcher")
        away_prob = g["teams"]["away"].get("probablePitcher")

        games.append({
            "gameTimeET": game_time_et,
            "homeTeam": home,
            "awayTeam": away,
            "ballparkKey": TEAM_TO_BALLPARK.get(home, home),
            "lineupComplete": lineup_complete,
            "topInning": {
                "teamBatting": away,
                "teamPitching": home,
                "pitcher": build_pitcher(home_prob),
                "lineup": build_lineup(away_players),
                "gameId": f"{abbrev(away)}/{abbrev(home)} - Top 1",
            },
            "botInning": {
                "teamBatting": home,
                "teamPitching": away,
                "pitcher": build_pitcher(away_prob),
                "lineup": build_lineup(home_players),
                "gameId": f"{abbrev(away)}/{abbrev(home)} - Bot 1",
            },
        })
    return games, None


# ── CSV data loading ──────────────────────────────────────────────────────────

@st.cache_data
def load_model_data():
    """Load and merge all CSV stat files, return (pitchers_df, batters_df, averages)."""
    import os
    base = os.path.dirname(__file__)

    def csv(name):
        return pd.read_csv(os.path.join(base, name))

    pvly = csv("pitching_vs_left_year.csv")
    pvry = csv("pitching_vs_right_year.csv")
    pvls = csv("pitching_vs_left_season.csv")
    pvrs = csv("pitching_vs_right_season.csv")
    bvly = csv("batting_vs_left_year.csv")
    bvry = csv("batting_vs_right_year.csv")
    bvls = csv("batting_vs_left_season.csv")
    bvrs = csv("batting_vs_right_season.csv")
    mlb  = csv("MLB_season.csv")

    # Rate columns — pitching
    for df in [pvly, pvry, pvls, pvrs]:
        df["K_Rate"]  = df["SO"]  / df["TBF"]
        df["BB_Rate"] = df["BB"]  / df["TBF"]
        df["Opp_1B"]  = (df["H"] - df["2B"] - df["3B"] - df["HR"]) / df["TBF"]
        df["2B_Rate"] = df["2B"] / df["TBF"]
        df["3B_Rate"] = df["3B"] / df["TBF"]
        df["HR_Rate"] = df["HR"] / df["TBF"]

    # Rate columns — batting
    for df in [bvly, bvry, bvls, bvrs]:
        df["K_Rate"]  = df["SO"] / df["PA"]
        df["BB_Rate"] = df["BB"] / df["PA"]
        df["1B_Rate"] = df["1B"] / df["PA"]
        df["2B_Rate"] = df["2B"] / df["PA"]
        df["3B_Rate"] = df["3B"] / df["PA"]
        df["HR_Rate"] = df["HR"] / df["PA"]

    # MLB averages
    mlb["K_Rate"]  = mlb["SO"] / mlb["TBF"]
    mlb["BB_Rate"] = mlb["BB"] / mlb["TBF"]
    mlb["1B_Rate"] = (mlb["H"] - mlb["2B"] - mlb["3B"] - mlb["HR"]) / mlb["TBF"]
    mlb["2B_Rate"] = mlb["2B"] / mlb["TBF"]
    mlb["3B_Rate"] = mlb["3B"] / mlb["TBF"]
    mlb["HR_Rate"] = mlb["HR"] / mlb["TBF"]
    avgs = {k: float(mlb[k].iloc[0]) for k in ["K_Rate","BB_Rate","1B_Rate","2B_Rate","3B_Rate","HR_Rate"]}

    # Merge pitchers
    p = pd.merge(pvly, pvry, on="Name", how="outer", suffixes=("_pvly","_pvry"))
    p = p.rename(columns={
        "K_Rate_pvly":"Year_K%_LHH","K_Rate_pvry":"Year_K%_RHH",
        "BB_Rate_pvly":"Year_BB_Rate_LHH","BB_Rate_pvry":"Year_BB_Rate_RHH",
        "Opp_1B_pvly":"Year_Opp_1B_LHH","Opp_1B_pvry":"Year_Opp_1B_RHH",
        "2B_Rate_pvly":"Year_2B_Rate_LHH","2B_Rate_pvry":"Year_2B_Rate_RHH",
        "3B_Rate_pvly":"Year_3B_Rate_LHH","3B_Rate_pvry":"Year_3B_Rate_RHH",
        "HR_Rate_pvly":"Year_HR_Rate_LHH","HR_Rate_pvry":"Year_HR_Rate_RHH",
    })
    drop_cols = [c for c in p.columns if c.endswith(("_pvly","_pvry")) and c != "Name"]
    p = p.drop(columns=drop_cols, errors="ignore")

    p = pd.merge(p, pvls, on="Name", how="outer")
    p = p.rename(columns={
        "K_Rate":"Season_K%_LHH","BB_Rate":"Season_BB_Rate_LHH",
        "Opp_1B":"Season_Opp_1B_LHH","2B_Rate":"Season_2B_Rate_LHH",
        "3B_Rate":"Season_3B_Rate_LHH","HR_Rate":"Season_HR_Rate_LHH",
    })
    drop_p = [c for c in p.columns if c not in ["Name"] and not c.startswith(("Year_","Season_","Handedness"))
              and c not in ["Year_K%_LHH","Year_K%_RHH"]]
    p = p.drop(columns=[c for c in drop_p if c in p.columns], errors="ignore")

    p = pd.merge(p, pvrs, on="Name", how="outer")
    p = p.rename(columns={
        "K_Rate":"Season_K%_RHH","BB_Rate":"Season_BB_Rate_RHH",
        "Opp_1B":"Season_Opp_1B_RHH","2B_Rate":"Season_2B_Rate_RHH",
        "3B_Rate":"Season_3B_Rate_RHH","HR_Rate":"Season_HR_Rate_RHH",
    })
    keep = [c for c in p.columns if c == "Name" or c.startswith(("Year_","Season_","Handedness"))]
    p = p[keep]
    p["Handedness"] = np.nan

    # Merge batters
    b = pd.merge(bvly, bvry, on="Name", how="outer", suffixes=("_bvly","_bvry"))
    b = b.rename(columns={
        "K_Rate_bvly":"Year_K%_LHP","K_Rate_bvry":"Year_K%_RHP",
        "BB_Rate_bvly":"Year_BB_Rate_LHP","BB_Rate_bvry":"Year_BB_Rate_RHP",
        "1B_Rate_bvly":"Year_1B_Rate_LHP","1B_Rate_bvry":"Year_1B_Rate_RHP",
        "2B_Rate_bvly":"Year_2B_Rate_LHP","2B_Rate_bvry":"Year_2B_Rate_RHP",
        "3B_Rate_bvly":"Year_3B_Rate_LHP","3B_Rate_bvry":"Year_3B_Rate_RHP",
        "HR_Rate_bvly":"Year_HR_Rate_LHP","HR_Rate_bvry":"Year_HR_Rate_RHP",
    })
    drop_cols2 = [c for c in b.columns if c.endswith(("_bvly","_bvry")) and c != "Name"]
    b = b.drop(columns=drop_cols2, errors="ignore")

    b = pd.merge(b, bvls, on="Name", how="outer")
    b = b.rename(columns={
        "K_Rate":"Season_K%_LHP","BB_Rate":"Season_BB_Rate_LHP",
        "1B_Rate":"Season_1B_Rate_LHP","2B_Rate":"Season_2B_Rate_LHP",
        "3B_Rate":"Season_3B_Rate_LHP","HR_Rate":"Season_HR_Rate_LHP",
    })
    keep_b = [c for c in b.columns if c == "Name" or c.startswith(("Year_","Season_","Handedness"))]
    b = b[keep_b]

    b = pd.merge(b, bvrs, on="Name", how="outer")
    b = b.rename(columns={
        "K_Rate":"Season_K%_RHP","BB_Rate":"Season_BB_Rate_RHP",
        "1B_Rate":"Season_1B_Rate_RHP","2B_Rate":"Season_2B_Rate_RHP",
        "3B_Rate":"Season_3B_Rate_RHP","HR_Rate":"Season_HR_Rate_RHP",
    })
    keep_b2 = [c for c in b.columns if c == "Name" or c.startswith(("Year_","Season_","Handedness"))]
    b = b[keep_b2]
    b["Handedness"] = np.nan

    # Handedness
    RHH = pd.read_csv(os.path.join(base,"RHH_data.csv"))
    LHH = pd.read_csv(os.path.join(base,"LHH_data.csv"))
    SWI = pd.read_csv(os.path.join(base,"switch_data.csv"))
    RHP = pd.read_csv(os.path.join(base,"RHP_data.csv"))
    LHP = pd.read_csv(os.path.join(base,"LHP_data.csv"))

    for name in RHP["Name"]:
        p.loc[p["Name"]==name, "Handedness"] = "R"
    for name in LHP["Name"]:
        p.loc[p["Name"]==name, "Handedness"] = "L"
    for name in RHH["Name"]:
        b.loc[b["Name"]==name, "Handedness"] = "R"
    for name in LHH["Name"]:
        b.loc[b["Name"]==name, "Handedness"] = "L"
    for name in SWI["Name"]:
        b.loc[b["Name"]==name, "Handedness"] = "S"

    return p, b, avgs


# ── Simulation engine (ported from notebook cell 16) ─────────────────────────

TEAM_NAME_TO_BALLPARK = {
    "Los Angeles Angels":"Angels","Arizona Diamondbacks":"Diamondbacks",
    "Baltimore Orioles":"Orioles","Boston Red Sox":"Red Sox",
    "Chicago Cubs":"Cubs","Chicago White Sox":"White Sox",
    "Cincinnati Reds":"Reds","Cleveland Guardians":"Guardians",
    "Colorado Rockies":"Rockies","Detroit Tigers":"Tigers",
    "Houston Astros":"Astros","Kansas City Royals":"Royals",
    "Los Angeles Dodgers":"Dodgers","Miami Marlins":"Marlins",
    "Milwaukee Brewers":"Brewers","Minnesota Twins":"Twins",
    "New York Mets":"Mets","New York Yankees":"Yankees",
    "Oakland Athletics":"Athletics","Sacramento Athletics":"Athletics",
    "Philadelphia Phillies":"Phillies","Pittsburgh Pirates":"Pirates",
    "San Diego Padres":"Padres","San Francisco Giants":"Giants",
    "Seattle Mariners":"Mariners","St. Louis Cardinals":"Cardinals",
    "Tampa Bay Rays":"Rays","Texas Rangers":"Rangers",
    "Toronto Blue Jays":"Blue Jays","Washington Nationals":"Nationals",
    "Atlanta Braves":"Braves",
}

PARK_FACTORS = {
    "Angels":{"1B_LH":0.95,"1B_RH":0.96,"2B_LH":0.91,"2B_RH":1.02,"3B_LH":0.55,"3B_RH":0.95,"HR_LH":1.29,"HR_RH":1.02},
    "Diamondbacks":{"1B_LH":1.05,"1B_RH":0.99,"2B_LH":1.01,"2B_RH":0.95,"3B_LH":2.39,"3B_RH":1.52,"HR_LH":0.97,"HR_RH":0.87},
    "Orioles":{"1B_LH":0.99,"1B_RH":1.00,"2B_LH":1.01,"2B_RH":0.87,"3B_LH":0.90,"3B_RH":0.65,"HR_LH":1.11,"HR_RH":1.20},
    "Red Sox":{"1B_LH":0.97,"1B_RH":0.99,"2B_LH":1.59,"2B_RH":1.25,"3B_LH":1.19,"3B_RH":1.21,"HR_LH":0.82,"HR_RH":0.97},
    "Cubs":{"1B_LH":1.03,"1B_RH":0.99,"2B_LH":0.98,"2B_RH":1.01,"3B_LH":1.18,"3B_RH":1.56,"HR_LH":0.83,"HR_RH":0.98},
    "White Sox":{"1B_LH":0.95,"1B_RH":1.03,"2B_LH":0.72,"2B_RH":0.91,"3B_LH":0.84,"3B_RH":0.31,"HR_LH":1.15,"HR_RH":1.12},
    "Reds":{"1B_LH":0.99,"1B_RH":0.93,"2B_LH":0.92,"2B_RH":1.08,"3B_LH":0.79,"3B_RH":0.63,"HR_LH":1.35,"HR_RH":1.30},
    "Guardians":{"1B_LH":0.99,"1B_RH":1.00,"2B_LH":1.13,"2B_RH":1.02,"3B_LH":0.85,"3B_RH":0.88,"HR_LH":1.08,"HR_RH":0.98},
    "Rockies":{"1B_LH":1.15,"1B_RH":1.19,"2B_LH":1.12,"2B_RH":1.43,"3B_LH":1.91,"3B_RH":2.17,"HR_LH":1.22,"HR_RH":1.21},
    "Tigers":{"1B_LH":0.98,"1B_RH":1.06,"2B_LH":0.83,"2B_RH":1.09,"3B_LH":1.69,"3B_RH":1.85,"HR_LH":0.88,"HR_RH":0.97},
    "Astros":{"1B_LH":0.98,"1B_RH":1.01,"2B_LH":0.91,"2B_RH":0.87,"3B_LH":1.27,"3B_RH":0.61,"HR_LH":1.05,"HR_RH":1.10},
    "Royals":{"1B_LH":1.15,"1B_RH":1.03,"2B_LH":1.22,"2B_RH":1.07,"3B_LH":1.17,"3B_RH":1.28,"HR_LH":0.76,"HR_RH":0.84},
    "Dodgers":{"1B_LH":0.96,"1B_RH":0.99,"2B_LH":1.06,"2B_RH":0.92,"3B_LH":0.24,"3B_RH":0.50,"HR_LH":1.04,"HR_RH":1.21},
    "Marlins":{"1B_LH":0.91,"1B_RH":1.09,"2B_LH":0.90,"2B_RH":1.04,"3B_LH":1.25,"3B_RH":0.99,"HR_LH":0.77,"HR_RH":0.72},
    "Brewers":{"1B_LH":0.96,"1B_RH":0.96,"2B_LH":0.91,"2B_RH":0.92,"3B_LH":0.82,"3B_RH":0.92,"HR_LH":1.08,"HR_RH":1.14},
    "Twins":{"1B_LH":1.03,"1B_RH":0.94,"2B_LH":1.03,"2B_RH":1.22,"3B_LH":1.40,"3B_RH":0.73,"HR_LH":0.89,"HR_RH":0.86},
    "Mets":{"1B_LH":1.01,"1B_RH":0.86,"2B_LH":0.74,"2B_RH":0.88,"3B_LH":0.62,"3B_RH":0.70,"HR_LH":0.98,"HR_RH":1.07},
    "Yankees":{"1B_LH":1.06,"1B_RH":1.05,"2B_LH":0.89,"2B_RH":0.85,"3B_LH":0.53,"3B_RH":1.36,"HR_LH":1.09,"HR_RH":1.02},
    "Athletics":{"1B_LH":1.00,"1B_RH":1.00,"2B_LH":1.00,"2B_RH":1.00,"3B_LH":1.00,"3B_RH":1.00,"HR_LH":1.00,"HR_RH":1.00},
    "Phillies":{"1B_LH":0.97,"1B_RH":0.98,"2B_LH":0.98,"2B_RH":0.88,"3B_LH":1.10,"3B_RH":0.99,"HR_LH":1.17,"HR_RH":1.22},
    "Pirates":{"1B_LH":0.97,"1B_RH":0.95,"2B_LH":1.27,"2B_RH":1.10,"3B_LH":0.75,"3B_RH":0.83,"HR_LH":0.93,"HR_RH":0.79},
    "Padres":{"1B_LH":0.95,"1B_RH":0.93,"2B_LH":1.07,"2B_RH":0.96,"3B_LH":0.76,"3B_RH":0.71,"HR_LH":0.92,"HR_RH":0.98},
    "Giants":{"1B_LH":0.97,"1B_RH":1.05,"2B_LH":1.05,"2B_RH":0.94,"3B_LH":1.66,"3B_RH":1.19,"HR_LH":0.73,"HR_RH":0.79},
    "Mariners":{"1B_LH":1.01,"1B_RH":0.95,"2B_LH":0.86,"2B_RH":0.83,"3B_LH":0.50,"3B_RH":0.75,"HR_LH":0.89,"HR_RH":1.04},
    "Cardinals":{"1B_LH":1.01,"1B_RH":1.05,"2B_LH":0.89,"2B_RH":0.89,"3B_LH":0.75,"3B_RH":1.10,"HR_LH":0.92,"HR_RH":0.84},
    "Rays":{"1B_LH":0.97,"1B_RH":0.96,"2B_LH":0.85,"2B_RH":1.01,"3B_LH":1.32,"3B_RH":1.22,"HR_LH":0.94,"HR_RH":0.86},
    "Rangers":{"1B_LH":1.04,"1B_RH":1.00,"2B_LH":1.01,"2B_RH":0.96,"3B_LH":1.01,"3B_RH":0.98,"HR_LH":0.95,"HR_RH":0.96},
    "Blue Jays":{"1B_LH":0.95,"1B_RH":0.92,"2B_LH":0.99,"2B_RH":1.02,"3B_LH":0.86,"3B_RH":1.03,"HR_LH":1.21,"HR_RH":1.12},
    "Nationals":{"1B_LH":1.01,"1B_RH":1.00,"2B_LH":1.30,"2B_RH":1.04,"3B_LH":0.85,"3B_RH":0.83,"HR_LH":1.14,"HR_RH":1.09},
    "Braves":{"1B_LH":0.99,"1B_RH":1.09,"2B_LH":1.04,"2B_RH":1.03,"3B_LH":0.69,"3B_RH":0.91,"HR_LH":0.90,"HR_RH":0.93},
}

def simulate_half_inning(pitcher_name, lineup, pitcher_hand_override,
                          batter_sides, team_batting, team_pitching, ballpark,
                          pitchers_df, batters_df, avgs, n=10000):
    """Run n-simulation Monte Carlo for one half inning. Returns probability dict."""

    avg_k  = avgs["K_Rate"]
    avg_bb = avgs["BB_Rate"]
    avg_1b = avgs["1B_Rate"]
    avg_2b = avgs["2B_Rate"]
    avg_3b = avgs["3B_Rate"]
    avg_hr = avgs["HR_Rate"]

    bp_key = TEAM_NAME_TO_BALLPARK.get(ballpark, ballpark)
    tb_key = TEAM_NAME_TO_BALLPARK.get(team_batting, team_batting)
    tp_key = TEAM_NAME_TO_BALLPARK.get(team_pitching, team_pitching)
    pf     = PARK_FACTORS

    # Build model_data rows: row 0 = pitcher, rows 1-9 = batters
    rows = []

    # Pitcher row
    if pitcher_name in pitchers_df["Name"].values:
        prow = pitchers_df[pitchers_df["Name"] == pitcher_name].iloc[0].to_dict()
    else:
        prow = {"Name": pitcher_name}
        for side in ["LHH","RHH"]:
            prow[f"Year_K%_{side}"]        = avg_k
            prow[f"Season_K%_{side}"]      = avg_k
            prow[f"Year_BB_Rate_{side}"]   = avg_bb
            prow[f"Season_BB_Rate_{side}"] = avg_bb
            prow[f"Year_Opp_1B_{side}"]    = avg_1b
            prow[f"Season_Opp_1B_{side}"]  = avg_1b
            for ht in ["2B","3B","HR"]:
                prow[f"Year_{ht}_Rate_{side}"]   = avgs[f"{ht}_Rate"]
                prow[f"Season_{ht}_Rate_{side}"] = avgs[f"{ht}_Rate"]
    prow["Handedness"] = pitcher_hand_override
    rows.append(prow)

    # Batter rows
    for i, bname in enumerate(lineup):
        if bname in batters_df["Name"].values:
            brow = batters_df[batters_df["Name"] == bname].iloc[0].to_dict()
        else:
            brow = {"Name": bname}
            for side in ["LHP","RHP"]:
                brow[f"Year_K%_{side}"]        = avg_k
                brow[f"Season_K%_{side}"]      = avg_k
                brow[f"Year_BB_Rate_{side}"]   = avg_bb
                brow[f"Season_BB_Rate_{side}"] = avg_bb
                brow[f"Year_1B_Rate_{side}"]   = avg_1b
                brow[f"Season_1B_Rate_{side}"] = avg_1b
                for ht in ["2B","3B","HR"]:
                    brow[f"Year_{ht}_Rate_{side}"]   = avgs[f"{ht}_Rate"]
                    brow[f"Season_{ht}_Rate_{side}"] = avgs[f"{ht}_Rate"]
        brow["Handedness"] = batter_sides[i] if i < len(batter_sides) else "R"
        rows.append(brow)

    model_data = pd.DataFrame(rows).reset_index(drop=True)
    handedness_backup = model_data["Handedness"].copy()
    model_data = model_data.apply(pd.to_numeric, errors="coerce")
    model_data["Handedness"] = handedness_backup

    def batter_suffix(bh, ph):
        if bh == "S":
            return "RHP" if ph == "L" else "LHP"
        return "LHP" if ph == "L" else "RHP"

    def pitcher_suffix(ph, bh):
        if bh == "S":
            return "LHH" if ph == "R" else "RHH"
        return "LHH" if bh == "L" else "RHH"

    def wsf(year, season, yw=0.4, sw=0.6):
        return year * yw + season * sw

    def csf(bv, pv, avg):
        bv = avg if (bv is None or np.isnan(bv)) else bv
        pv = avg if (pv is None or np.isnan(pv)) else pv
        return (bv + pv) / 2

    # Precompute per-batter stats
    pitcher_hand = model_data.at[0, "Handedness"]
    batter_stats_list = []
    for ri in range(1, 10):
        bh = model_data.at[ri, "Handedness"]
        ps = pitcher_suffix(pitcher_hand, bh)
        bs = batter_suffix(bh, pitcher_hand)

        def pget(col):
            v = model_data.at[0, col] if col in model_data.columns else np.nan
            return float(v) if not pd.isna(v) else np.nan

        def bget(col):
            v = model_data.at[ri, col] if col in model_data.columns else np.nan
            return float(v) if not pd.isna(v) else np.nan

        p_k  = wsf(pget(f"Year_K%_{ps}"),        pget(f"Season_K%_{ps}"))
        p_bb = wsf(pget(f"Year_BB_Rate_{ps}"),    pget(f"Season_BB_Rate_{ps}"))
        p_1b = wsf(pget(f"Year_Opp_1B_{ps}"),     pget(f"Season_Opp_1B_{ps}"))
        p_2b = wsf(pget(f"Year_2B_Rate_{ps}"),    pget(f"Season_2B_Rate_{ps}"))
        p_3b = wsf(pget(f"Year_3B_Rate_{ps}"),    pget(f"Season_3B_Rate_{ps}"))
        p_hr = wsf(pget(f"Year_HR_Rate_{ps}"),    pget(f"Season_HR_Rate_{ps}"))

        b_k  = wsf(bget(f"Year_K%_{bs}"),         bget(f"Season_K%_{bs}"))
        b_bb = wsf(bget(f"Year_BB_Rate_{bs}"),     bget(f"Season_BB_Rate_{bs}"))
        b_1b = wsf(bget(f"Year_1B_Rate_{bs}"),     bget(f"Season_1B_Rate_{bs}"))
        b_2b = wsf(bget(f"Year_2B_Rate_{bs}"),     bget(f"Season_2B_Rate_{bs}"))
        b_3b = wsf(bget(f"Year_3B_Rate_{bs}"),     bget(f"Season_3B_Rate_{bs}"))
        b_hr = wsf(bget(f"Year_HR_Rate_{bs}"),     bget(f"Season_HR_Rate_{bs}"))

        # Park adjustments
        hand_key = "RH" if (bh == "R" or (bh == "S" and pitcher_hand == "L")) else "LH"
        if bp_key in pf:
            if bp_key == tb_key:  # home batter
                for stat_name, b_val_name, p_val_name in [
                    ("1b", "b_1b", "p_1b"), ("2b", "b_2b", "p_2b"),
                    ("3b", "b_3b", "p_3b"), ("hr", "b_hr", "p_hr")
                ]:
                    factor = pf[bp_key].get(f"{stat_name.upper()}_{hand_key}", 1.0)
                    locals()[b_val_name] *= (0.5 * factor + 0.5)
                    locals()[p_val_name] *= (0.5 * factor + 0.5)
            else:  # away batter
                if tb_key in pf:
                    for stat_name, b_val_name in [("1b","b_1b"),("2b","b_2b"),("3b","b_3b"),("hr","b_hr")]:
                        bp_f = pf[bp_key].get(f"{stat_name.upper()}_{hand_key}", 1.0)
                        tb_f = pf[tb_key].get(f"{stat_name.upper()}_{hand_key}", 1.0)
                        locals()[b_val_name] *= (1 + (tb_f - bp_f) * 0.5)
                if tp_key in pf:
                    for stat_name, p_val_name in [("1b","p_1b"),("2b","p_2b"),("3b","p_3b"),("hr","p_hr")]:
                        bp_f = pf[bp_key].get(f"{stat_name.upper()}_{hand_key}", 1.0)
                        tp_f = pf[tp_key].get(f"{stat_name.upper()}_{hand_key}", 1.0)
                        locals()[p_val_name] *= (1 + (tp_f - bp_f) * 0.5)

        ck  = csf(b_k,  p_k,  avg_k)
        cbb = csf(b_bb, p_bb, avg_bb)
        c1b = csf(b_1b, p_1b, avg_1b)
        c2b = csf(b_2b, p_2b, avg_2b)
        c3b = csf(b_3b, p_3b, avg_3b)
        chr_ = csf(b_hr, p_hr, avg_hr)

        total = ck + cbb + c1b + c2b + c3b + chr_
        if total > 0:
            ck /= total; cbb /= total; c1b /= total
            c2b /= total; c3b /= total; chr_ /= total
        else:
            ck = avg_k; cbb = avg_bb; c1b = avg_1b
            c2b = avg_2b; c3b = avg_3b; chr_ = avg_hr

        cip = max(0, 1 - ck - cbb)
        batter_stats_list.append((ck, cbb, cip, c1b, c2b, c3b, chr_))

    # Run simulations
    no_k = no_h = over3 = 0
    for _ in range(n):
        outs = ks = hits = batters = 0
        r1 = r2 = r3 = False
        ri = 0  # batter index 0-8
        while outs < 3:
            batters += 1
            ck, cbb, cip, c1b, c2b, c3b, chr_ = batter_stats_list[ri % 9]
            outcome = np.random.rand()
            if outcome < ck:
                ks += 1; outs += 1
            elif outcome < ck + cbb:
                if r1 and r2: r3 = True
                elif r1: r2 = True
                else: r1 = True
            else:
                ip = (outcome - ck - cbb) / cip if cip > 0 else 0
                if ip < c1b / cip if cip > 0 else 0:
                    hits += 1
                    if r1 and r2: r3 = True
                    elif r1: r2 = True
                    else: r1 = True
                elif ip < (c1b + c2b) / cip if cip > 0 else 0:
                    hits += 1; r3 = True; r2 = True; r1 = False
                elif ip < (c1b + c2b + c3b) / cip if cip > 0 else 0:
                    hits += 1; r3 = True; r2 = r1 = False
                elif ip < (c1b + c2b + c3b + chr_) / cip if cip > 0 else 0:
                    hits += 1; r1 = r2 = r3 = False
                else:
                    if r1 and outs < 2 and np.random.rand() < 0.064:
                        outs += 2; r1 = r2 = r3 = False
                    else:
                        outs += 1
            ri += 1
        if ks == 0: no_k += 1
        if hits == 0: no_h += 1
        if batters <= 3: over3 += 1

    return {
        "p_nsfi": no_k / n,
        "p_no_hits": no_h / n,
        "p_under4": over3 / n,
    }


# ── Main dashboard ────────────────────────────────────────────────────────────

today = datetime.now().strftime("%Y-%m-%d")

# Sidebar
with st.sidebar:
    st.markdown("### Settings")
    n_sims = st.slider("Simulations", 1000, 20000, 10000, 1000,
                        help="Monte Carlo simulations per half-inning")
    min_ev = st.slider("Min EV threshold (%)", 0, 15, 5, 1,
                        help="Only highlight bets above this expected value")
    show_all = st.checkbox("Include games without odds", value=False)
    st.markdown("---")
    if st.button("Refresh All Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown("##### How it works")
    st.markdown(
        "The model runs **10,000 Monte Carlo simulations** per half-inning "
        "using pitcher/batter splits and park factors, then compares the "
        "predicted P(NSFI) against DraftKings implied odds to find **+EV bets**."
    )
    st.markdown("---")
    st.caption("Built with MLB Stats API + DraftKings sportsbook-nash API")

# Load data
pitchers_df, batters_df, avgs = load_model_data()
games, games_err = fetch_today_games(today)

if games_err:
    st.error(f"Failed to fetch games: {games_err}")
    st.stop()
if not games:
    st.warning("No MLB regular season games found for today.")
    st.stop()

dk_odds, dk_source, dk_err = fetch_dk_odds(today)
if dk_err:
    dk_odds = {}

# Status bar
complete = sum(1 for g in games if g["lineupComplete"])
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Games Today", len(games))
with col2:
    st.metric("Lineups Posted", f"{complete}/{len(games)}")
with col3:
    dk_count = len(dk_odds) if dk_odds else 0
    st.metric("DK Markets", dk_count)

if dk_err:
    st.warning(f"DraftKings odds unavailable: {dk_err}")
elif dk_source:
    st.caption(f"Odds source: {dk_source}")

st.markdown("---")

results = []
progress = st.progress(0)
half_innings = [(g, slot) for g in games for slot in ["topInning", "botInning"]
                if g["lineupComplete"]]
total = len(half_innings)

for idx, (game, slot) in enumerate(half_innings):
    half = game[slot]
    game_id = half["gameId"]
    pitcher = half["pitcher"]
    lineup_players = half["lineup"]

    if len(lineup_players) < 9:
        progress.progress((idx + 1) / max(total, 1))
        continue

    lineup_names = [p["name"] for p in lineup_players]
    batter_sides = [p["batSide"] for p in lineup_players]

    batting_team = half["teamBatting"]
    pitching_team = half["teamPitching"]
    dk_entry = dk_odds.get(pitching_team) if dk_odds else None
    implied_nsfi = dk_entry["impliedNSFI"] if dk_entry else None
    dk_no_odds   = dk_entry["noOdds"]      if dk_entry else None
    dk_yes_odds  = dk_entry["yesOdds"]     if dk_entry else None

    if not show_all and implied_nsfi is None:
        progress.progress((idx + 1) / max(total, 1))
        continue

    sim = simulate_half_inning(
        pitcher_name=pitcher["name"],
        lineup=lineup_names,
        pitcher_hand_override=pitcher["pitchHand"],
        batter_sides=batter_sides,
        team_batting=batting_team,
        team_pitching=half["teamPitching"],
        ballpark=game["ballparkKey"],
        pitchers_df=pitchers_df,
        batters_df=batters_df,
        avgs=avgs,
        n=n_sims,
    )

    ev_nsfi = (sim["p_nsfi"] - implied_nsfi) if implied_nsfi is not None else None

    def fmt_odds(o):
        if o is None: return "—"
        return f"+{o}" if o > 0 else str(o)

    row = {
        "Game": game_id,
        "Time": game["gameTimeET"],
        "Batting": batting_team,
        "Pitcher": pitcher["name"],
        "P Model": f"{sim['p_nsfi']*100:.1f}%",
        "P Implied": f"{implied_nsfi*100:.1f}%" if implied_nsfi else "—",
        "EV": f"{ev_nsfi*100:+.1f}%" if ev_nsfi is not None else "—",
        "DK No": fmt_odds(dk_no_odds),
        "DK Yes": fmt_odds(dk_yes_odds),
        "_ev_raw": ev_nsfi if ev_nsfi is not None else -999,
        "_has_odds": implied_nsfi is not None,
    }
    results.append(row)
    progress.progress((idx + 1) / max(total, 1))

progress.empty()

if not results:
    st.warning("No results to display. DraftKings odds may not be posted yet, or no lineups are complete.")
    st.stop()

df = pd.DataFrame(results)

# ── BET RECOMMENDATIONS ──────────────────────────────────────────────────────

min_ev_dec = min_ev / 100
strong = df[df["_ev_raw"] >= min_ev_dec].copy().sort_values("_ev_raw", ascending=False)
marginal = df[(df["_ev_raw"] >= 0) & (df["_ev_raw"] < min_ev_dec) & df["_has_odds"]].copy().sort_values("_ev_raw", ascending=False)
avoid = df[(df["_ev_raw"] < 0) & df["_has_odds"]].copy().sort_values("_ev_raw", ascending=False)

st.markdown("### Recommended Bets")

if not strong.empty:
    st.markdown(f"**{len(strong)} bet(s)** with EV above {min_ev}%")
    for _, row in strong.iterrows():
        ev_val = row["_ev_raw"] * 100
        st.markdown(f"""<div class="bet-take">
            <strong>{row['Game']}</strong> &nbsp; <span style="color:#888">{row['Time']}</span><br>
            <span style="color:#ccc">Pitcher: {row['Pitcher']}</span><br>
            <span style="font-size:1.3rem">
                Model: <strong>{row['P Model']}</strong> &nbsp;|&nbsp;
                DK Implied: {row['P Implied']} &nbsp;|&nbsp;
                DK No: <strong>{row['DK No']}</strong> &nbsp;|&nbsp;
                EV: <span class="ev-positive">{ev_val:+.1f}%</span>
            </span>
        </div>""", unsafe_allow_html=True)
else:
    st.info(f"No bets above {min_ev}% EV right now. Try lowering the threshold or check back closer to game time.")

# ── MARGINAL BETS ────────────────────────────────────────────────────────────

if not marginal.empty:
    st.markdown("---")
    st.markdown("### Marginal Bets (0% to {}% EV)".format(min_ev))
    for _, row in marginal.iterrows():
        ev_val = row["_ev_raw"] * 100
        st.markdown(f"""<div class="bet-marginal">
            <strong>{row['Game']}</strong> &nbsp; <span style="color:#888">{row['Time']}</span><br>
            <span style="color:#ccc">Pitcher: {row['Pitcher']}</span><br>
            <span style="font-size:1.1rem">
                Model: <strong>{row['P Model']}</strong> &nbsp;|&nbsp;
                DK Implied: {row['P Implied']} &nbsp;|&nbsp;
                DK No: <strong>{row['DK No']}</strong> &nbsp;|&nbsp;
                EV: <span class="ev-marginal">{ev_val:+.1f}%</span>
            </span>
        </div>""", unsafe_allow_html=True)

# ── BETS TO AVOID ────────────────────────────────────────────────────────────

if not avoid.empty:
    st.markdown("---")
    with st.expander(f"Negative EV ({len(avoid)} half-innings) — avoid", expanded=False):
        for _, row in avoid.iterrows():
            ev_val = row["_ev_raw"] * 100
            st.markdown(f"""<div class="bet-skip">
                <strong>{row['Game']}</strong> &nbsp; <span style="color:#888">{row['Time']}</span><br>
                <span style="color:#ccc">Pitcher: {row['Pitcher']}</span><br>
                <span style="font-size:1.1rem">
                    Model: <strong>{row['P Model']}</strong> &nbsp;|&nbsp;
                    DK Implied: {row['P Implied']} &nbsp;|&nbsp;
                    DK No: {row['DK No']} &nbsp;|&nbsp;
                    EV: <span class="ev-negative">{ev_val:+.1f}%</span>
                </span>
            </div>""", unsafe_allow_html=True)

# ── GAMES WITHOUT ODDS ───────────────────────────────────────────────────────

no_odds = df[~df["_has_odds"]].copy()
if show_all and not no_odds.empty:
    st.markdown("---")
    with st.expander(f"Games without DraftKings odds ({len(no_odds)})", expanded=False):
        st.dataframe(
            no_odds[["Game", "Time", "Pitcher", "Batting", "P Model"]].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )

# ── FOOTER ───────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption(
    "Model: 10K Monte Carlo sims using 2024-25 MLB Stats API splits + Swish Analytics park factors \u2022 "
    "Odds: DraftKings sportsbook-nash API \u2022 "
    "Updated automatically via GitHub Actions"
)
