#!/usr/bin/env python3
"""
run_model.py — Run the NSFI simulation model headlessly and save predictions.

Reads today's daily JSON (lineups + DK odds), runs Monte Carlo simulations,
and saves model outputs to model_predictions_YYYYMMDD.json for later grading
by track_results.py.

Usage:
  python3 run_model.py                    # run for today
  python3 run_model.py --date 2026-03-27
  python3 run_model.py --sims 5000        # fewer sims for speed
"""

import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ── Odds helpers ──────────────────────────────────────────────────────────────

def american_to_implied(odds: int) -> float:
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)


# ── CSV data loading (no Streamlit) ───────────────────────────────────────────

def load_model_data():
    def csv(name):
        return pd.read_csv(os.path.join(BASE_DIR, name))

    pvly = csv("pitching_vs_left_year.csv")
    pvry = csv("pitching_vs_right_year.csv")
    pvls = csv("pitching_vs_left_season.csv")
    pvrs = csv("pitching_vs_right_season.csv")
    bvly = csv("batting_vs_left_year.csv")
    bvry = csv("batting_vs_right_year.csv")
    bvls = csv("batting_vs_left_season.csv")
    bvrs = csv("batting_vs_right_season.csv")
    mlb  = csv("MLB_season.csv")

    for df in [pvly, pvry, pvls, pvrs]:
        df["K_Rate"]  = df["SO"]  / df["TBF"]
        df["BB_Rate"] = df["BB"]  / df["TBF"]
        df["Opp_1B"]  = (df["H"] - df["2B"] - df["3B"] - df["HR"]) / df["TBF"]
        df["2B_Rate"] = df["2B"] / df["TBF"]
        df["3B_Rate"] = df["3B"] / df["TBF"]
        df["HR_Rate"] = df["HR"] / df["TBF"]

    for df in [bvly, bvry, bvls, bvrs]:
        df["K_Rate"]  = df["SO"] / df["PA"]
        df["BB_Rate"] = df["BB"] / df["PA"]
        df["1B_Rate"] = df["1B"] / df["PA"]
        df["2B_Rate"] = df["2B"] / df["PA"]
        df["3B_Rate"] = df["3B"] / df["PA"]
        df["HR_Rate"] = df["HR"] / df["PA"]

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
    keep = [c for c in p.columns if c == "Name" or c.startswith(("Year_","Season_"))]
    p = p[keep]
    p = pd.merge(p, pvrs, on="Name", how="outer")
    p = p.rename(columns={
        "K_Rate":"Season_K%_RHH","BB_Rate":"Season_BB_Rate_RHH",
        "Opp_1B":"Season_Opp_1B_RHH","2B_Rate":"Season_2B_Rate_RHH",
        "3B_Rate":"Season_3B_Rate_RHH","HR_Rate":"Season_HR_Rate_RHH",
    })
    keep2 = [c for c in p.columns if c == "Name" or c.startswith(("Year_","Season_"))]
    p = p[keep2]
    p["Handedness"] = pd.Series([np.nan] * len(p), dtype="object")

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
    keep_b = [c for c in b.columns if c == "Name" or c.startswith(("Year_","Season_"))]
    b = b[keep_b]
    b = pd.merge(b, bvrs, on="Name", how="outer")
    b = b.rename(columns={
        "K_Rate":"Season_K%_RHP","BB_Rate":"Season_BB_Rate_RHP",
        "1B_Rate":"Season_1B_Rate_RHP","2B_Rate":"Season_2B_Rate_RHP",
        "3B_Rate":"Season_3B_Rate_RHP","HR_Rate":"Season_HR_Rate_RHP",
    })
    keep_b2 = [c for c in b.columns if c == "Name" or c.startswith(("Year_","Season_"))]
    b = b[keep_b2]
    b["Handedness"] = pd.Series([np.nan] * len(b), dtype="object")

    # Handedness
    for name in pd.read_csv(os.path.join(BASE_DIR, "RHP_data.csv"))["Name"]:
        p.loc[p["Name"]==name, "Handedness"] = "R"
    for name in pd.read_csv(os.path.join(BASE_DIR, "LHP_data.csv"))["Name"]:
        p.loc[p["Name"]==name, "Handedness"] = "L"
    for name in pd.read_csv(os.path.join(BASE_DIR, "RHH_data.csv"))["Name"]:
        b.loc[b["Name"]==name, "Handedness"] = "R"
    for name in pd.read_csv(os.path.join(BASE_DIR, "LHH_data.csv"))["Name"]:
        b.loc[b["Name"]==name, "Handedness"] = "L"
    for name in pd.read_csv(os.path.join(BASE_DIR, "switch_data.csv"))["Name"]:
        b.loc[b["Name"]==name, "Handedness"] = "S"

    return p, b, avgs


# ── Simulation (identical to model.py) ────────────────────────────────────────

# Import PARK_FACTORS and TEAM_NAME_TO_BALLPARK from model.py source
# to avoid duplication — but since model.py imports streamlit, we inline them.

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
    avg_k  = avgs["K_Rate"];  avg_bb = avgs["BB_Rate"]
    avg_1b = avgs["1B_Rate"]; avg_2b = avgs["2B_Rate"]
    avg_3b = avgs["3B_Rate"]; avg_hr = avgs["HR_Rate"]

    bp_key = TEAM_NAME_TO_BALLPARK.get(ballpark, ballpark)
    tb_key = TEAM_NAME_TO_BALLPARK.get(team_batting, team_batting)
    tp_key = TEAM_NAME_TO_BALLPARK.get(team_pitching, team_pitching)
    pf = PARK_FACTORS

    rows = []
    if pitcher_name in pitchers_df["Name"].values:
        prow = pitchers_df[pitchers_df["Name"] == pitcher_name].iloc[0].to_dict()
    else:
        prow = {"Name": pitcher_name}
        for side in ["LHH","RHH"]:
            prow[f"Year_K%_{side}"] = avg_k;  prow[f"Season_K%_{side}"] = avg_k
            prow[f"Year_BB_Rate_{side}"] = avg_bb; prow[f"Season_BB_Rate_{side}"] = avg_bb
            prow[f"Year_Opp_1B_{side}"] = avg_1b;  prow[f"Season_Opp_1B_{side}"] = avg_1b
            for ht in ["2B","3B","HR"]:
                prow[f"Year_{ht}_Rate_{side}"] = avgs[f"{ht}_Rate"]
                prow[f"Season_{ht}_Rate_{side}"] = avgs[f"{ht}_Rate"]
    prow["Handedness"] = pitcher_hand_override
    rows.append(prow)

    for i, bname in enumerate(lineup):
        if bname in batters_df["Name"].values:
            brow = batters_df[batters_df["Name"] == bname].iloc[0].to_dict()
        else:
            brow = {"Name": bname}
            for side in ["LHP","RHP"]:
                brow[f"Year_K%_{side}"] = avg_k;  brow[f"Season_K%_{side}"] = avg_k
                brow[f"Year_BB_Rate_{side}"] = avg_bb; brow[f"Season_BB_Rate_{side}"] = avg_bb
                brow[f"Year_1B_Rate_{side}"] = avg_1b;  brow[f"Season_1B_Rate_{side}"] = avg_1b
                for ht in ["2B","3B","HR"]:
                    brow[f"Year_{ht}_Rate_{side}"] = avgs[f"{ht}_Rate"]
                    brow[f"Season_{ht}_Rate_{side}"] = avgs[f"{ht}_Rate"]
        brow["Handedness"] = batter_sides[i] if i < len(batter_sides) else "R"
        rows.append(brow)

    model_data = pd.DataFrame(rows).reset_index(drop=True)
    handedness_backup = model_data["Handedness"].copy()
    model_data = model_data.apply(pd.to_numeric, errors="coerce")
    model_data["Handedness"] = handedness_backup

    def batter_suffix(bh, ph):
        return ("RHP" if ph == "L" else "LHP") if bh == "S" else ("LHP" if ph == "L" else "RHP")
    def pitcher_suffix(ph, bh):
        return ("LHH" if ph == "R" else "RHH") if bh == "S" else ("LHH" if bh == "L" else "RHH")
    def wsf(year, season): return year * 0.4 + season * 0.6
    def csf(bv, pv, avg):
        bv = avg if (bv is None or np.isnan(bv)) else bv
        pv = avg if (pv is None or np.isnan(pv)) else pv
        return (bv + pv) / 2

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

        p_k = wsf(pget(f"Year_K%_{ps}"), pget(f"Season_K%_{ps}"))
        p_bb = wsf(pget(f"Year_BB_Rate_{ps}"), pget(f"Season_BB_Rate_{ps}"))
        p_1b = wsf(pget(f"Year_Opp_1B_{ps}"), pget(f"Season_Opp_1B_{ps}"))
        p_2b = wsf(pget(f"Year_2B_Rate_{ps}"), pget(f"Season_2B_Rate_{ps}"))
        p_3b = wsf(pget(f"Year_3B_Rate_{ps}"), pget(f"Season_3B_Rate_{ps}"))
        p_hr = wsf(pget(f"Year_HR_Rate_{ps}"), pget(f"Season_HR_Rate_{ps}"))
        b_k = wsf(bget(f"Year_K%_{bs}"), bget(f"Season_K%_{bs}"))
        b_bb = wsf(bget(f"Year_BB_Rate_{bs}"), bget(f"Season_BB_Rate_{bs}"))
        b_1b = wsf(bget(f"Year_1B_Rate_{bs}"), bget(f"Season_1B_Rate_{bs}"))
        b_2b = wsf(bget(f"Year_2B_Rate_{bs}"), bget(f"Season_2B_Rate_{bs}"))
        b_3b = wsf(bget(f"Year_3B_Rate_{bs}"), bget(f"Season_3B_Rate_{bs}"))
        b_hr = wsf(bget(f"Year_HR_Rate_{bs}"), bget(f"Season_HR_Rate_{bs}"))

        hand_key = "RH" if (bh == "R" or (bh == "S" and pitcher_hand == "L")) else "LH"
        if bp_key in pf:
            if bp_key == tb_key:
                for ht in ["1B","2B","3B","HR"]:
                    f_ = pf[bp_key].get(f"{ht}_{hand_key}", 1.0)
                    adj = 0.5 * f_ + 0.5
                    if ht == "1B": b_1b *= adj
                    elif ht == "2B": b_2b *= adj
                    elif ht == "3B": b_3b *= adj
                    elif ht == "HR": b_hr *= adj
                if tp_key in pf:
                    for ht in ["1B","2B","3B","HR"]:
                        bp_f = pf[bp_key].get(f"{ht}_{hand_key}", 1.0)
                        tp_f = pf[tp_key].get(f"{ht}_{hand_key}", 1.0)
                        adj = 1 + (tp_f - bp_f) * 0.5
                        if ht == "1B": p_1b *= adj
                        elif ht == "2B": p_2b *= adj
                        elif ht == "3B": p_3b *= adj
                        elif ht == "HR": p_hr *= adj
            else:
                if tb_key in pf:
                    for ht in ["1B","2B","3B","HR"]:
                        bp_f = pf[bp_key].get(f"{ht}_{hand_key}", 1.0)
                        tb_f = pf[tb_key].get(f"{ht}_{hand_key}", 1.0)
                        adj = 1 + (tb_f - bp_f) * 0.5
                        if ht == "1B": b_1b *= adj
                        elif ht == "2B": b_2b *= adj
                        elif ht == "3B": b_3b *= adj
                        elif ht == "HR": b_hr *= adj
                for ht in ["1B","2B","3B","HR"]:
                    f_ = pf[bp_key].get(f"{ht}_{hand_key}", 1.0)
                    adj = 0.5 * f_ + 0.5
                    if ht == "1B": p_1b *= adj
                    elif ht == "2B": p_2b *= adj
                    elif ht == "3B": p_3b *= adj
                    elif ht == "HR": p_hr *= adj

        ck = csf(b_k, p_k, avg_k); cbb = csf(b_bb, p_bb, avg_bb)
        c1b = csf(b_1b, p_1b, avg_1b); c2b = csf(b_2b, p_2b, avg_2b)
        c3b = csf(b_3b, p_3b, avg_3b); chr_ = csf(b_hr, p_hr, avg_hr)
        cip = max(0, 1 - ck - cbb)
        total_rate = ck + cbb + cip
        if total_rate > 0 and total_rate != 1.0:
            ck /= total_rate; cbb /= total_rate; cip = max(0, 1 - ck - cbb)
        if ck == 0 and cbb == 0 and cip == 0:
            ck = avg_k; cbb = avg_bb; c1b = avg_1b
            c2b = avg_2b; c3b = avg_3b; chr_ = avg_hr
            cip = max(0, 1 - ck - cbb)
        batter_stats_list.append((ck, cbb, cip, c1b, c2b, c3b, chr_))

    no_k = no_h = over3 = 0
    for _ in range(n):
        outs = ks = hits = batters = 0
        r1 = r2 = r3 = False
        ri = 0
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
                hit_1b = (c1b / cip) if cip > 0 else 0
                hit_2b = ((c1b + c2b) / cip) if cip > 0 else 0
                hit_3b = ((c1b + c2b + c3b) / cip) if cip > 0 else 0
                hit_hr = ((c1b + c2b + c3b + chr_) / cip) if cip > 0 else 0
                if ip < hit_1b:
                    hits += 1
                    if r1 and r2: r3 = True
                    elif r1: r2 = True
                    else: r1 = True
                elif ip < hit_2b:
                    hits += 1; r3 = True; r2 = True; r1 = False
                elif ip < hit_3b:
                    hits += 1; r3 = True; r2 = r1 = False
                elif ip < hit_hr:
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

    return {"p_nsfi": no_k / n, "p_no_hits": no_h / n, "p_under4": over3 / n}


# ── Main ──────────────────────────────────────────────────────────────────────

def run(date_str, n_sims=10000):
    daily_path = os.path.join(BASE_DIR, f"daily_{date_str.replace('-', '')}.json")
    out_path = os.path.join(BASE_DIR, f"model_predictions_{date_str.replace('-', '')}.json")

    if not os.path.exists(daily_path):
        print(f"No daily JSON found for {date_str}.")
        return

    with open(daily_path) as f:
        daily = json.load(f)

    print(f"Loading model data...")
    pitchers_df, batters_df, avgs = load_model_data()

    predictions = []
    for game in daily.get("games", []):
        if not game.get("lineupComplete"):
            continue

        dk = (game.get("odds") or {}).get("draftkings") or {}

        for slot, half_key in [("top", "topInning"), ("bot", "botInning")]:
            half = game[half_key]
            pitcher = half["pitcher"]
            lineup = half["lineup"]
            if len(lineup) < 9:
                continue

            dk_slot = dk.get(slot, {})
            if not dk_slot.get("oddsPosted"):
                continue

            no_odds = dk_slot.get("noOdds")
            implied_nsfi = dk_slot.get("impliedNSFI")
            if no_odds is None or implied_nsfi is None:
                continue

            sim = simulate_half_inning(
                pitcher_name=pitcher["name"],
                lineup=[p["name"] for p in lineup],
                pitcher_hand_override=pitcher["pitchHand"],
                batter_sides=[p["batSide"] for p in lineup],
                team_batting=half["teamBatting"],
                team_pitching=half["teamPitching"],
                ballpark=game["ballparkKey"],
                pitchers_df=pitchers_df,
                batters_df=batters_df,
                avgs=avgs,
                n=n_sims,
            )

            ev = sim["p_nsfi"] - implied_nsfi
            if ev >= 0.05:
                ev_category = "strong"
            elif ev >= 0:
                ev_category = "marginal"
            else:
                ev_category = "negative"

            pred = {
                "game_id": half["gameId"],
                "half": slot,
                "pitcher": pitcher["name"],
                "batting_team": half["teamBatting"],
                "pitching_team": half["teamPitching"],
                "model_prob": round(sim["p_nsfi"], 4),
                "implied_prob": implied_nsfi,
                "ev": round(ev, 4),
                "ev_category": ev_category,
                "dk_no_odds": no_odds,
            }
            predictions.append(pred)

            ev_pct = ev * 100
            no_str = f"+{no_odds}" if no_odds > 0 else str(no_odds)
            cat_label = {"strong": "TAKE", "marginal": "WATCH", "negative": "SKIP"}[ev_category]
            print(f"  {half['gameId']}: model={sim['p_nsfi']:.1%} implied={implied_nsfi:.1%} "
                  f"EV={ev_pct:+.1f}% DK No:{no_str} [{cat_label}]")

    if not predictions:
        print("No predictions to save (no complete lineups with DK odds).")
        return

    with open(out_path, "w") as f:
        json.dump({
            "date": date_str,
            "generatedAt": datetime.now().isoformat(),
            "simulations": n_sims,
            "predictions": predictions,
        }, f, indent=2)

    strong = sum(1 for p in predictions if p["ev_category"] == "strong")
    marginal = sum(1 for p in predictions if p["ev_category"] == "marginal")
    negative = sum(1 for p in predictions if p["ev_category"] == "negative")
    print(f"\nSaved {len(predictions)} predictions: {strong} TAKE, {marginal} WATCH, {negative} SKIP")
    print(f"  -> {out_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", default=None, help="Date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--sims", type=int, default=10000, help="Simulations per half-inning.")
    args = parser.parse_args()
    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    run(date_str, n_sims=args.sims)


if __name__ == "__main__":
    main()
