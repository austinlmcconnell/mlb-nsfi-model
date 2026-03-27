#!/usr/bin/env python3
"""
fetch_daily.py — MLB lineup + DraftKings NSFI odds fetcher

Fetches today's (or any date's) regular season lineups from the MLB Stats API
and first-inning strikeout odds from DraftKings via headless browser, then
saves structured data ready for the NSFI model.

DraftKings offers a "Strikeout Thrown - 1st Inning" Yes/No market per
half-inning under each game's 1st Inning > Strikeouts tab. The "No" outcome
gives the direct P(NSFI) for each half-inning.

Usage:
  python3 fetch_daily.py                     # fetch once for today
  python3 fetch_daily.py --date 2025-03-27
  python3 fetch_daily.py --poll              # keep polling until all lineups post
  python3 fetch_daily.py --poll --interval 10
  python3 fetch_daily.py --no-dk             # skip DraftKings odds
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta

import requests

# ── MLB Stats API ────────────────────────────────────────────────────────────

MLB_BASE = "https://statsapi.mlb.com/api/v1"

# MLB API full name → NSFI model ballpark key
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


# ── MLB lineup helpers ───────────────────────────────────────────────────────

def fetch_schedule(date_str):
    data = api_get(MLB_BASE + "/schedule", params={
        "sportId": 1,
        "date": date_str,
        "hydrate": "lineups,probablePitcher,team,venue",
        "gameType": "R",
    })
    dates = data.get("dates", [])
    return dates[0].get("games", []) if dates else []


def resolve_handedness(player_ids, cache):
    missing = [pid for pid in player_ids if pid not in cache]
    if not missing:
        return
    chunk_size = 150
    for i in range(0, len(missing), chunk_size):
        chunk = missing[i:i + chunk_size]
        data = api_get(MLB_BASE + "/people",
                       params={"personIds": ",".join(str(p) for p in chunk)})
        for p in data.get("people", []):
            cache[p["id"]] = {
                "batSide": p.get("batSide", {}).get("code", "R"),
                "pitchHand": p.get("pitchHand", {}).get("code", "R"),
                "fullName": p.get("fullName", ""),
            }


def parse_game(game, handedness_cache):
    game_pk = game["gamePk"]
    home_team = game["teams"]["home"]["team"]["name"]
    away_team = game["teams"]["away"]["team"]["name"]
    venue = game.get("venue", {}).get("name", home_team)
    game_time_utc = game.get("gameDate", "")

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

    all_ids = [p["id"] for p in home_players_raw + away_players_raw]
    for prob in [home_prob, away_prob]:
        if prob:
            all_ids.append(prob["id"])
    resolve_handedness(all_ids, handedness_cache)

    def build_pitcher(prob):
        if not prob:
            return {"name": "TBD", "id": None, "pitchHand": "R"}
        h = handedness_cache.get(prob["id"], {})
        return {"name": prob["fullName"], "id": prob["id"],
                "pitchHand": h.get("pitchHand", "R")}

    def build_lineup(players_raw):
        return [{
            "name": p["fullName"],
            "id": p["id"],
            "batSide": handedness_cache.get(p["id"], {}).get("batSide", "R"),
            "position": p.get("primaryPosition", {}).get("abbreviation", ""),
        } for p in players_raw]

    abbrev = lambda name: TEAM_TO_ABBREV.get(name, name[:3].upper())

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
        "topInning": {
            "teamBatting": away_team,
            "teamPitching": home_team,
            "pitcher": build_pitcher(home_prob),
            "lineup": build_lineup(away_players_raw),
            "gameId": f"{abbrev(away_team)}/{abbrev(home_team)} - Top 1",
        },
        "botInning": {
            "teamBatting": home_team,
            "teamPitching": away_team,
            "pitcher": build_pitcher(away_prob),
            "lineup": build_lineup(home_players_raw),
            "gameId": f"{abbrev(away_team)}/{abbrev(home_team)} - Bot 1",
        },
    }


def american_to_implied(odds: int) -> float:
    """Convert American odds integer to implied probability (0–1)."""
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)


def implied_to_american(prob: float) -> str:
    """Convert implied probability to American odds string."""
    if prob <= 0 or prob >= 1:
        return "N/A"
    if prob >= 0.5:
        return f"{round(-(prob / (1 - prob)) * 100)}"
    return f"+{round(((1 - prob) / prob) * 100)}"


# ── DraftKings odds (sportsbook-nash API) ────────────────────────────────────

DK_NASH_BASE = "https://sportsbook-nash.draftkings.com/api/sportscontent/dkusoh/v1"
DK_LEAGUE_ID = "84240"         # MLB regular season
DK_CATEGORY_ID = "1024"        # 1st Inning
DK_SUBCATEGORY_ID = "12855"    # Strikeouts

# DK short team names → canonical MLB API full names
DK_TEAM_MAP = {
    "NY Yankees": "New York Yankees", "NY Mets": "New York Mets",
    "LA Dodgers": "Los Angeles Dodgers", "LA Angels": "Los Angeles Angels",
    "SF Giants": "San Francisco Giants", "SD Padres": "San Diego Padres",
    "TB Rays": "Tampa Bay Rays", "KC Royals": "Kansas City Royals",
    "CWS White Sox": "Chicago White Sox", "CHI White Sox": "Chicago White Sox",
    "CHI Cubs": "Chicago Cubs", "CLE Guardians": "Cleveland Guardians",
    "CIN Reds": "Cincinnati Reds", "COL Rockies": "Colorado Rockies",
    "DET Tigers": "Detroit Tigers", "HOU Astros": "Houston Astros",
    "MIA Marlins": "Miami Marlins", "MIL Brewers": "Milwaukee Brewers",
    "MIN Twins": "Minnesota Twins", "PHI Phillies": "Philadelphia Phillies",
    "PIT Pirates": "Pittsburgh Pirates", "SEA Mariners": "Seattle Mariners",
    "STL Cardinals": "St. Louis Cardinals", "TEX Rangers": "Texas Rangers",
    "TOR Blue Jays": "Toronto Blue Jays", "WSH Nationals": "Washington Nationals",
    "ATL Braves": "Atlanta Braves", "ARI Diamondbacks": "Arizona Diamondbacks",
    "BAL Orioles": "Baltimore Orioles", "BOS Red Sox": "Boston Red Sox",
    "Athletics": "Sacramento Athletics",
}


def _fetch_via_browser(url, timeout=30):
    """Fetch JSON from a URL using a headless browser (Chrome or Edge).
    Used as fallback when requests is blocked by TLS fingerprinting."""
    import platform
    from selenium import webdriver

    driver = None
    # Try Chrome first (available on Linux/GitHub Actions), then Edge (Windows)
    browsers = []
    if platform.system() == "Windows":
        browsers = ["edge", "chrome"]
    else:
        browsers = ["chrome", "edge"]

    for browser in browsers:
        try:
            if browser == "chrome":
                from selenium.webdriver.chrome.options import Options
                opts = Options()
            else:
                from selenium.webdriver.edge.options import Options
                opts = Options()
            opts.add_argument("--headless=new")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--log-level=3")
            if browser == "chrome":
                driver = webdriver.Chrome(options=opts)
            else:
                driver = webdriver.Edge(options=opts)
            break
        except Exception:
            continue

    if driver is None:
        raise RuntimeError("No browser available (install Chrome or Edge)")

    try:
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        # The page source wraps JSON in <pre> tags
        import re as _re
        body = driver.find_element("tag name", "pre").text
        return json.loads(body)
    finally:
        driver.quit()


def fetch_all_dk_nsfi(max_games=0) -> dict:
    """
    Fetch DraftKings NSFI odds via the sportsbook-nash API.

    Single request returns all "Strikeout Thrown - 1st Inning" Yes/No markets
    for every MLB game. No browser or auth required.

    Returns dict keyed by canonical team name:
      {team: {oddsPosted, noOdds, yesOdds, impliedNSFI, source}}
    """
    url = (f"{DK_NASH_BASE}/leagues/{DK_LEAGUE_ID}"
           f"/categories/{DK_CATEGORY_ID}/subcategories/{DK_SUBCATEGORY_ID}")

    print(f"  [DraftKings] Fetching from sportsbook-nash API…", end=" ", flush=True)

    # Try requests first (works on Mac), fall back to Selenium Edge (Windows)
    data = None
    try:
        data = api_get(url)
        print("OK (via requests).", flush=True)
    except Exception:
        print("requests blocked, trying browser…", end=" ", flush=True)
        try:
            data = _fetch_via_browser(url)
            print("OK (via browser).", flush=True)
        except Exception as e:
            print(f"failed: {e}")
            return {}

    if not data:
        print("no data returned.")
        return {}

    markets = {m["id"]: m for m in data.get("markets", [])}
    selections = data.get("selections", [])

    # Build a map: marketId → {yes_odds, no_odds, team_name}
    market_odds = {}
    for sel in selections:
        mid = sel.get("marketId")
        if mid not in markets:
            continue
        market = markets[mid]
        name = market.get("name", "")
        if "Strikeout Thrown - 1st Inning" not in name:
            continue

        label = sel.get("label", "")
        odds_str = sel.get("displayOdds", {}).get("american", "")
        if not odds_str:
            continue
        # DK uses unicode minus "−" not ASCII "-"
        odds_str = odds_str.replace("\u2212", "-").replace("−", "-")
        try:
            odds_val = int(odds_str)
        except ValueError:
            continue

        if mid not in market_odds:
            # Extract team name from market name
            team_dk = name.replace("Strikeout Thrown - 1st Inning", "").strip()
            market_odds[mid] = {"team": team_dk, "yes": None, "no": None}

        if label == "Yes":
            market_odds[mid]["yes"] = odds_val
        elif label == "No":
            market_odds[mid]["no"] = odds_val

    # Convert to results dict keyed by canonical team name
    results = {}
    for mid, info in market_odds.items():
        if info["no"] is None:
            continue
        # Map DK short name to canonical full name
        team = DK_TEAM_MAP.get(info["team"], info["team"])
        results[team] = {
            "oddsPosted": True,
            "noOdds": info["no"],
            "yesOdds": info["yes"],
            "impliedNSFI": round(american_to_implied(info["no"]), 4),
            "source": "draftkings_nash",
        }

    if results:
        print(f"got {len(results)} markets.")
        for team, info in sorted(results.items()):
            no_str = f"+{info['noOdds']}" if info['noOdds'] > 0 else str(info['noOdds'])
            print(f"    {team}: No {no_str}  P(NSFI)={info['impliedNSFI']:.1%}")
    else:
        print("no markets found (odds may not be posted yet).")

    return results


# ── Combined run loop ────────────────────────────────────────────────────────

def run(date_str, poll=False, interval_min=15, use_dk=True, test_mode=False):
    out_file = os.path.join(
        os.path.dirname(__file__), f"daily_{date_str.replace('-', '')}.json"
    )
    handedness_cache = {}

    # Fetch all DraftKings NSFI odds at once via headless browser
    dk_all = {}
    if use_dk:
        print("  Fetching DraftKings odds…", flush=True)
        try:
            dk_all = fetch_all_dk_nsfi(max_games=1 if test_mode else 0)
        except Exception as e:
            print(f"  [DraftKings] {e}")
            use_dk = False

    attempt = 0
    while True:
        attempt += 1
        now_et = datetime.now(timezone.utc) - timedelta(hours=4)
        print(f"\n[{now_et.strftime('%H:%M ET')}] Attempt {attempt} — {date_str}")

        # ── Lineups ──────────────────────────────────────────────────────────
        print("  Fetching MLB lineups…", end=" ", flush=True)
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

        games_data = [parse_game(g, handedness_cache) for g in raw_games]
        complete_count = sum(1 for g in games_data if g["lineupComplete"])
        print(f"{len(games_data)} games, {complete_count} lineups complete.")

        # ── DraftKings odds ────────────────────────────────────────────────
        odds_found = 0
        for g in games_data:
            dk_specials = {}
            if use_dk and dk_all:
                for slot, pitching_team in [("top", g["homeTeam"]), ("bot", g["awayTeam"])]:
                    entry = dk_all.get(pitching_team)
                    if entry and entry.get("oddsPosted"):
                        dk_specials[slot] = entry
                    else:
                        dk_specials[slot] = {"oddsPosted": False}

            g["odds"] = {"draftkings": dk_specials or None}

            dk_ok = dk_specials.get("top", {}).get("oddsPosted") or \
                    dk_specials.get("bot", {}).get("oddsPosted")
            if dk_ok:
                odds_found += 1

        print(f"  {odds_found}/{len(games_data)} games have DraftKings odds.")

        # ── Save ─────────────────────────────────────────────────────────────
        with open(out_file, "w") as f:
            json.dump({
                "date": date_str,
                "fetchedAt": datetime.now(timezone.utc).isoformat(),
                "games": games_data,
            }, f, indent=2)

        _print_summary(games_data, date_str)

        if complete_count == len(games_data):
            print(f"All {len(games_data)} lineups complete. Saved → {out_file}")
            break

        if not poll:
            print(f"Saved partial data ({complete_count}/{len(games_data)} lineups complete) → {out_file}")
            break

        wait_until = now_et + timedelta(minutes=interval_min)
        pending = len(games_data) - complete_count
        print(f"  {pending} lineup(s) still pending. "
              f"Checking again at {wait_until.strftime('%H:%M ET')}…")
        time.sleep(interval_min * 60)


def _print_summary(games_data, date_str):
    complete = [g for g in games_data if g["lineupComplete"]]
    pending  = [g for g in games_data if not g["lineupComplete"]]

    print(f"\n{'='*70}")
    print(f"  MLB NSFI DAILY — {date_str}")
    print(f"  {len(complete)}/{len(games_data)} lineups complete")
    print(f"{'='*70}")

    for g in sorted(games_data, key=lambda x: x["gameTimeUTC"]):
        abbr = lambda n: TEAM_TO_ABBREV.get(n, n[:3].upper())
        lu_status = "✓" if g["lineupComplete"] else f"⏳ drops ~{g['lineupDropET']}"
        print(f"\n  {g['awayTeam']} @ {g['homeTeam']}  {g['gameTimeET']}  [{lu_status}]")

        for slot, label in [("top", "Top 1"), ("bot", "Bot 1")]:
            half = g["topInning"] if slot == "top" else g["botInning"]
            pitcher = half["pitcher"]
            hand = "RHP" if pitcher["pitchHand"] == "R" else "LHP"

            lineup_str = ""
            if g["lineupComplete"]:
                lineup_str = ", ".join(
                    f"{p['name']} ({p['batSide']})" for p in half["lineup"][:3]
                ) + "…"

            dk_slot = (g.get("odds", {}).get("draftkings") or {}).get(slot, {})

            print(f"    {label}: {half['teamBatting']} vs {pitcher['name']} ({hand})")

            if dk_slot.get("oddsPosted"):
                no_odds = dk_slot["noOdds"]
                yes_odds = dk_slot.get("yesOdds")
                nsfi_p = dk_slot["impliedNSFI"]
                no_str = f"+{no_odds}" if no_odds > 0 else str(no_odds)
                yes_str = (f"+{yes_odds}" if yes_odds and yes_odds > 0 else str(yes_odds)) if yes_odds else "?"
                print(f"         DK NSFI No: {no_str}  Yes: {yes_str}  P(NSFI)={nsfi_p:.1%}")
            else:
                print(f"         [odds pending]")

            if lineup_str:
                print(f"         {lineup_str}")

    if pending:
        print(f"\n  Lineups pending: " + ", ".join(
            f"{TEAM_TO_ABBREV.get(g['awayTeam'], g['awayTeam'][:3])}@"
            f"{TEAM_TO_ABBREV.get(g['homeTeam'], g['homeTeam'][:3])}"
            for g in pending
        ))
    print()


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--date", default=None,
                        help="Date to fetch (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--poll", action="store_true",
                        help="Keep polling until all lineups are posted.")
    parser.add_argument("--interval", type=int, default=15,
                        help="Polling interval in minutes (default: 15).")
    parser.add_argument("--no-dk", action="store_true",
                        help="Skip DraftKings odds.")
    parser.add_argument("--test", action="store_true",
                        help="Test mode: only process the first game.")
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    run(date_str, poll=args.poll, interval_min=args.interval,
        use_dk=not args.no_dk, test_mode=args.test)


if __name__ == "__main__":
    main()
