#!/usr/bin/env python3
"""
fetch_lineups.py — MLB lineup and probable pitcher fetcher

Pulls today's (or any date's) regular season games from the MLB Stats API,
resolves player handedness, and saves structured data ready for the NSFI model.

Usage:
  python3 fetch_lineups.py                  # fetch today's lineups once
  python3 fetch_lineups.py --date 2025-03-27
  python3 fetch_lineups.py --poll           # keep polling until all lineups post
  python3 fetch_lineups.py --poll --interval 10  # poll every 10 minutes (default: 15)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import requests

BASE = "https://statsapi.mlb.com/api/v1"

# Maps MLB API full team names → NSFI model's team_rates / TEAM_NAME_TO_BALLPARK keys
# (same mapping already embedded in the notebook's TEAM_NAME_TO_BALLPARK dict)
TEAM_TO_ABBREV = {
    "Los Angeles Angels": "LAA",
    "Arizona Diamondbacks": "ARI",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Oakland Athletics": "OAK",
    "Sacramento Athletics": "OAK",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
    "Atlanta Braves": "ATL",
}

TEAM_TO_BALLPARK = {
    "Los Angeles Angels": "Angels",
    "Arizona Diamondbacks": "Diamondbacks",
    "Baltimore Orioles": "Orioles",
    "Boston Red Sox": "Red Sox",
    "Chicago Cubs": "Cubs",
    "Chicago White Sox": "White Sox",
    "Cincinnati Reds": "Reds",
    "Cleveland Guardians": "Guardians",
    "Colorado Rockies": "Rockies",
    "Detroit Tigers": "Tigers",
    "Houston Astros": "Astros",
    "Kansas City Royals": "Royals",
    "Los Angeles Dodgers": "Dodgers",
    "Miami Marlins": "Marlins",
    "Milwaukee Brewers": "Brewers",
    "Minnesota Twins": "Twins",
    "New York Mets": "Mets",
    "New York Yankees": "Yankees",
    "Oakland Athletics": "Athletics",
    "Sacramento Athletics": "Athletics",
    "Philadelphia Phillies": "Phillies",
    "Pittsburgh Pirates": "Pirates",
    "San Diego Padres": "Padres",
    "San Francisco Giants": "Giants",
    "Seattle Mariners": "Mariners",
    "St. Louis Cardinals": "Cardinals",
    "Tampa Bay Rays": "Rays",
    "Texas Rangers": "Rangers",
    "Toronto Blue Jays": "Blue Jays",
    "Washington Nationals": "Nationals",
    "Atlanta Braves": "Braves",
}


def api_get(path, params=None, retries=3):
    url = BASE + path if path.startswith("/") else path
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"API call failed ({url}): {e}") from e


def fetch_schedule(date_str):
    """Return list of regular season game dicts for the given date."""
    data = api_get("/schedule", params={
        "sportId": 1,
        "date": date_str,
        "hydrate": "lineups,probablePitcher,team,venue",
        "gameType": "R",
    })
    dates = data.get("dates", [])
    return dates[0].get("games", []) if dates else []


def resolve_handedness(player_ids):
    """
    Batch-fetch bat side / pitch hand for a list of MLB player IDs.
    Returns dict: {player_id: {'batSide': 'L'|'R'|'S', 'pitchHand': 'L'|'R'}}
    """
    if not player_ids:
        return {}
    chunk_size = 150  # API limit per request
    result = {}
    for i in range(0, len(player_ids), chunk_size):
        chunk = player_ids[i:i + chunk_size]
        data = api_get("/people", params={"personIds": ",".join(str(pid) for pid in chunk)})
        for p in data.get("people", []):
            result[p["id"]] = {
                "batSide": p.get("batSide", {}).get("code", "R"),
                "pitchHand": p.get("pitchHand", {}).get("code", "R"),
                "fullName": p.get("fullName", ""),
            }
    return result


def parse_game(game, handedness_cache):
    """
    Convert a raw MLB API game dict into the structured format for the NSFI model.
    Returns None if the game doesn't have a complete lineup yet.
    """
    game_pk = game["gamePk"]
    home_team = game["teams"]["home"]["team"]["name"]
    away_team = game["teams"]["away"]["team"]["name"]
    venue = game.get("venue", {}).get("name", home_team)
    game_time_utc = game.get("gameDate", "")  # ISO 8601 UTC string

    # Convert to ET for display (UTC-4 during EDT)
    try:
        dt_utc = datetime.fromisoformat(game_time_utc.replace("Z", "+00:00"))
        dt_et = dt_utc - timedelta(hours=4)
        game_time_et = dt_et.strftime("%-I:%M %p ET")
        lineup_drop_et = (dt_et - timedelta(hours=3, minutes=30)).strftime("%-I:%M %p ET")
    except Exception:
        game_time_et = game_time_utc
        lineup_drop_et = "unknown"

    home_prob = game["teams"]["home"].get("probablePitcher")
    away_prob = game["teams"]["away"].get("probablePitcher")

    lineups = game.get("lineups", {})
    home_players_raw = lineups.get("homePlayers", [])
    away_players_raw = lineups.get("awayPlayers", [])

    lineup_complete = (len(home_players_raw) == 9 and len(away_players_raw) == 9)

    # Collect all player IDs we need handedness for
    all_ids = []
    for p in home_players_raw + away_players_raw:
        all_ids.append(p["id"])
    for prob in [home_prob, away_prob]:
        if prob:
            all_ids.append(prob["id"])

    # Fetch any IDs not already in cache
    missing = [pid for pid in all_ids if pid not in handedness_cache]
    if missing:
        new_data = resolve_handedness(missing)
        handedness_cache.update(new_data)

    def build_pitcher(prob):
        if not prob:
            return {"name": "TBD", "id": None, "pitchHand": "R"}
        h = handedness_cache.get(prob["id"], {})
        return {
            "name": prob["fullName"],
            "id": prob["id"],
            "pitchHand": h.get("pitchHand", "R"),
        }

    def build_lineup(players_raw):
        lineup = []
        for p in players_raw:
            h = handedness_cache.get(p["id"], {})
            lineup.append({
                "name": p["fullName"],
                "id": p["id"],
                "batSide": h.get("batSide", "R"),
                "position": p.get("primaryPosition", {}).get("abbreviation", ""),
            })
        return lineup

    return {
        "gamePk": game_pk,
        "gameTimeUTC": game_time_utc,
        "gameTimeET": game_time_et,
        "lineupDropET": lineup_drop_et,
        "lineupComplete": lineup_complete,
        "homeTeam": home_team,
        "awayTeam": away_team,
        "venue": venue,
        "ballparkKey": TEAM_TO_BALLPARK.get(home_team, home_team),
        # top of 1st: away bats
        "topInning": {
            "teamBatting": away_team,
            "teamPitching": home_team,
            "pitcher": build_pitcher(home_prob),
            "lineup": build_lineup(away_players_raw),
            "gameId": f"{TEAM_TO_ABBREV.get(away_team, away_team[:3].upper())}/{TEAM_TO_ABBREV.get(home_team, home_team[:3].upper())} - Top 1",
        },
        # bottom of 1st: home bats
        "botInning": {
            "teamBatting": home_team,
            "teamPitching": away_team,
            "pitcher": build_pitcher(away_prob),
            "lineup": build_lineup(home_players_raw),
            "gameId": f"{TEAM_TO_ABBREV.get(away_team, away_team[:3].upper())}/{TEAM_TO_ABBREV.get(home_team, home_team[:3].upper())} - Bot 1",
        },
    }


def print_summary(games_data, date_str):
    """Print a human-readable summary of fetched lineups."""
    complete = [g for g in games_data if g["lineupComplete"]]
    pending  = [g for g in games_data if not g["lineupComplete"]]

    print(f"\n{'='*60}")
    print(f"  MLB LINEUPS — {date_str}")
    print(f"  {len(complete)}/{len(games_data)} games have complete lineups")
    print(f"{'='*60}")

    for g in sorted(games_data, key=lambda x: x["gameTimeUTC"]):
        status = "✓" if g["lineupComplete"] else f"⏳ (drops ~{g['lineupDropET']})"
        print(f"\n  {g['awayTeam']} @ {g['homeTeam']}  {g['gameTimeET']}  {status}")
        if g["lineupComplete"]:
            t = g["topInning"]
            b = g["botInning"]
            print(f"    Top 1: {t['teamBatting']} bat vs {t['pitcher']['name']} "
                  f"({'RHP' if t['pitcher']['pitchHand'] == 'R' else 'LHP'})")
            print(f"           " + ", ".join(
                f"{p['name']} ({p['batSide']})" for p in t["lineup"][:3]
            ) + "…")
            print(f"    Bot 1: {b['teamBatting']} bat vs {b['pitcher']['name']} "
                  f"({'RHP' if b['pitcher']['pitchHand'] == 'R' else 'LHP'})")
            print(f"           " + ", ".join(
                f"{p['name']} ({p['batSide']})" for p in b["lineup"][:3]
            ) + "…")

    if pending:
        print(f"\n  Pending ({len(pending)}):", ", ".join(
            f"{TEAM_TO_ABBREV.get(g['awayTeam'], g['awayTeam'][:3])}@{TEAM_TO_ABBREV.get(g['homeTeam'], g['homeTeam'][:3])}" for g in pending
        ))
    print()


def run(date_str, poll=False, interval_min=15):
    out_file = os.path.join(os.path.dirname(__file__), f"lineups_{date_str.replace('-', '')}.json")
    handedness_cache = {}

    attempt = 0
    while True:
        attempt += 1
        now_et = datetime.now(timezone.utc) - timedelta(hours=4)
        print(f"[{now_et.strftime('%H:%M ET')}] Fetching games for {date_str} "
              f"(attempt {attempt})…", end=" ", flush=True)

        try:
            raw_games = fetch_schedule(date_str)
        except RuntimeError as e:
            print(f"ERROR: {e}")
            if not poll:
                sys.exit(1)
            time.sleep(interval_min * 60)
            continue

        if not raw_games:
            print("No regular season games found.")
            if not poll:
                break
            time.sleep(interval_min * 60)
            continue

        print(f"{len(raw_games)} games found.")

        games_data = [parse_game(g, handedness_cache) for g in raw_games]

        # Save output
        with open(out_file, "w") as f:
            json.dump({
                "date": date_str,
                "fetchedAt": datetime.now(timezone.utc).isoformat(),
                "games": games_data,
            }, f, indent=2)

        print_summary(games_data, date_str)

        complete_count = sum(1 for g in games_data if g["lineupComplete"])

        if complete_count == len(games_data):
            print(f"All {len(games_data)} lineups complete. Saved → {out_file}")
            break

        if not poll:
            print(f"Saved partial lineups ({complete_count}/{len(games_data)} complete) → {out_file}")
            break

        wait_until = now_et + timedelta(minutes=interval_min)
        print(f"  {len(games_data) - complete_count} lineup(s) still pending. "
              f"Checking again at {wait_until.strftime('%H:%M ET')}…")
        time.sleep(interval_min * 60)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", default=None,
                        help="Date to fetch (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--poll", action="store_true",
                        help="Keep polling until all lineups are posted.")
    parser.add_argument("--interval", type=int, default=15,
                        help="Polling interval in minutes (default: 15).")
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    run(date_str, poll=args.poll, interval_min=args.interval)


if __name__ == "__main__":
    main()
