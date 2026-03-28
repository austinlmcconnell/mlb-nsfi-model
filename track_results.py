#!/usr/bin/env python3
"""
track_results.py — Track NSFI model results against actual game outcomes.

Checks completed MLB games for the given date, determines if a strikeout
occurred in each half-inning's 1st inning, and records results against
the model's predictions in historical_results.csv.

Usage:
  python3 track_results.py                  # track today's results
  python3 track_results.py --date 2026-03-27
"""

import argparse
import csv
import json
import os
import requests
import time
from datetime import datetime


MLB_BASE = "https://statsapi.mlb.com/api/v1"

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


def api_get(url, params=None, retries=3, timeout=20):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"Request failed ({url}): {e}") from e


def american_to_implied(odds: int) -> float:
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)


def check_first_inning_strikeouts(game_pk):
    """
    Check if strikeouts occurred in the 1st inning of a game.
    Returns dict: {"top": bool, "bot": bool} — True means strikeout occurred (NSFI loss).
    """
    data = api_get(f"{MLB_BASE}/game/{game_pk}/playByPlay")

    result = {"top": False, "bot": False}
    for play in data.get("allPlays", []):
        about = play.get("about", {})
        if about.get("inning", 0) != 1:
            continue
        half = about.get("halfInning", "")
        event_type = play.get("result", {}).get("eventType", "")
        if event_type == "strikeout":
            slot = "top" if half == "top" else "bot"
            result[slot] = True

    return result


def load_daily_json(date_str):
    """Load the daily JSON file for the given date."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base_dir, f"daily_{date_str.replace('-', '')}.json")
    if not os.path.exists(json_path):
        return None
    with open(json_path) as f:
        return json.load(f)


def load_model_predictions(date_str):
    """Load model predictions for the given date."""
    pred_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             f"model_predictions_{date_str.replace('-', '')}.json")
    if not os.path.exists(pred_path):
        return {}
    with open(pred_path) as f:
        data = json.load(f)
    # Build lookup: game_id|half -> prediction
    return {f"{p['game_id']}|{p['half']}": p for p in data.get("predictions", [])}


def load_existing_results():
    """Load existing historical results to avoid duplicates."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "historical_results.csv")
    existing = set()
    if os.path.exists(csv_path):
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = f"{row.get('date', '')}|{row.get('game_id', '')}|{row.get('half', '')}"
                existing.add(key)
    return existing


def run(date_str):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "historical_results.csv")

    # Load daily JSON for model predictions and odds
    daily = load_daily_json(date_str)
    if not daily:
        print(f"No daily JSON found for {date_str}. Run fetch_daily.py first.")
        return

    # Load existing results to skip duplicates
    existing = load_existing_results()

    # Load model predictions for EV categorization
    model_preds = load_model_predictions(date_str)

    # Get completed games from MLB API
    schedule = api_get(f"{MLB_BASE}/schedule", params={
        "sportId": 1, "date": date_str, "gameType": "R",
    })
    dates = schedule.get("dates", [])
    if not dates:
        print(f"No games found for {date_str}.")
        return

    mlb_games = dates[0].get("games", [])
    completed = [g for g in mlb_games if g["status"]["detailedState"] == "Final"]
    print(f"Found {len(completed)}/{len(mlb_games)} completed games for {date_str}.")

    if not completed:
        print("No completed games to track yet.")
        return

    # Build lookup from daily JSON: gamePk -> game data with odds
    daily_by_pk = {}
    for g in daily.get("games", []):
        daily_by_pk[g["gamePk"]] = g

    # Process each completed game
    new_rows = []
    for mlb_game in completed:
        game_pk = mlb_game["gamePk"]
        home = mlb_game["teams"]["home"]["team"]["name"]
        away = mlb_game["teams"]["away"]["team"]["name"]

        # Find in daily JSON
        daily_game = daily_by_pk.get(game_pk)
        if not daily_game:
            print(f"  {away} @ {home}: no daily data (skipping)")
            continue

        # Check actual 1st inning results
        try:
            k_results = check_first_inning_strikeouts(game_pk)
        except Exception as e:
            print(f"  {away} @ {home}: failed to get play-by-play ({e})")
            continue

        dk = (daily_game.get("odds") or {}).get("draftkings") or {}
        abbrev = lambda n: TEAM_TO_ABBREV.get(n, n[:3].upper())

        for slot, half_key in [("top", "topInning"), ("bot", "botInning")]:
            half = daily_game.get(half_key, {})
            game_id = half.get("gameId", f"{abbrev(away)}/{abbrev(home)} - {'Top' if slot == 'top' else 'Bot'} 1")

            # Check for duplicate
            dup_key = f"{date_str}|{game_id}|{slot}"
            if dup_key in existing:
                continue

            dk_slot = dk.get(slot, {})
            if not dk_slot.get("oddsPosted"):
                continue

            no_odds = dk_slot.get("noOdds")
            implied_nsfi = dk_slot.get("impliedNSFI")
            if no_odds is None or implied_nsfi is None:
                continue

            pitcher = half.get("pitcher", {}).get("name", "Unknown")
            batting_team = half.get("teamBatting", "")
            pitching_team = half.get("teamPitching", "")

            # NSFI result: win if NO strikeout in this half
            had_strikeout = k_results.get(slot, True)
            nsfi_result = "win" if not had_strikeout else "loss"

            # Look up model prediction for EV category
            pred_key = f"{game_id}|{slot}"
            pred = model_preds.get(pred_key, {})
            model_prob = pred.get("model_prob", "")
            ev = pred.get("ev", "")
            ev_category = pred.get("ev_category", "")

            row = {
                "date": date_str,
                "game_id": game_id,
                "half": slot,
                "pitcher": pitcher,
                "batting_team": batting_team,
                "pitching_team": pitching_team,
                "model_prob": model_prob,
                "implied_prob": implied_nsfi,
                "ev": ev,
                "dk_no_odds": no_odds,
                "result": nsfi_result,
                "ev_category": ev_category,
            }
            new_rows.append(row)

            emoji = "W" if nsfi_result == "win" else "L"
            no_str = f"+{no_odds}" if no_odds > 0 else str(no_odds)
            print(f"  {game_id}: [{emoji}] NSFI {'hit' if nsfi_result == 'win' else 'missed'} "
                  f"(DK No: {no_str}, implied: {implied_nsfi:.1%})")

    if not new_rows:
        print("No new results to record.")
        return

    # Write to CSV
    fieldnames = ["date", "game_id", "half", "pitcher", "batting_team", "pitching_team",
                  "model_prob", "implied_prob", "ev", "dk_no_odds", "result", "ev_category"]

    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0
    # Check if existing file has the right headers
    write_header = True
    if file_exists:
        with open(csv_path, "r") as f:
            first_line = f.readline().strip()
            if first_line and "date" in first_line:
                write_header = False

    with open(csv_path, "a" if not write_header else "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)

    wins = sum(1 for r in new_rows if r["result"] == "win")
    losses = len(new_rows) - wins
    print(f"\nRecorded {len(new_rows)} results: {wins}W-{losses}L")
    print(f"Saved to {csv_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", default=None,
                        help="Date to track results for (YYYY-MM-DD). Defaults to today.")
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    run(date_str)


if __name__ == "__main__":
    main()
