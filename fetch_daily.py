#!/usr/bin/env python3
"""
fetch_daily.py — Combined MLB lineup + FanDuel NSFI odds fetcher

Fetches today's (or any date's) regular season lineups from the MLB Stats API
and first-inning strikeout odds from FanDuel, then saves structured data ready
for the NSFI model.

FanDuel does not offer an explicit "No Strikeout" market, but prices
"1+ Strikeout" per half-inning under "Specials Top/Bottom 1st". The implied
P(NSFI) for each half-inning is derived as:
    P(NSFI) = 1 − P(1+ Strikeout)

Usage:
  python3 fetch_daily.py                     # fetch once for today
  python3 fetch_daily.py --date 2025-03-27
  python3 fetch_daily.py --poll              # keep polling until all lineups post
  python3 fetch_daily.py --poll --interval 10
  python3 fetch_daily.py --state nj          # use New Jersey FanDuel endpoint
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

# FanDuel may use shorter names; normalize to canonical MLB API names
FD_TEAM_ALIASES = {
    "Athletics": "Sacramento Athletics",
    "Oakland Athletics": "Sacramento Athletics",
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


# ── FanDuel odds helpers ─────────────────────────────────────────────────────

FD_BASE = "https://sbapi.{state}.sportsbook.fanduel.com/api"
FD_PARAMS = {
    "betexRegion": "GBR",
    "capiJurisdiction": "intl",
    "currencyCode": "USD",
    "exchangeLocale": "en_US",
    "language": "en",
    "regionCode": "NAMERICA",
    "_ak": "FhMFpcPWXMeyZxOx",
}

# Regex: "Away Team (P Initial) @ Home Team (P Initial)"
_FD_NAME_RE = re.compile(r"^(.+?)\s+\(.+?\)\s+@\s+(.+?)\s+\(.+?\)$")


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


def normalize_fd_name(name: str) -> str:
    """Map FanDuel team name variants to canonical MLB API names."""
    return FD_TEAM_ALIASES.get(name, name)


def fetch_fd_events(state: str) -> dict:
    """
    Returns dict mapping (away_team_canonical, home_team_canonical) → fd_event_id
    for today's MLB regular season games on FanDuel.
    """
    base = FD_BASE.format(state=state)
    try:
        data = api_get(base + "/content-managed-page",
                       params={**FD_PARAMS, "page": "CUSTOM", "customPageId": "mlb"})
    except RuntimeError as e:
        print(f"  [FanDuel] content-managed-page failed: {e}")
        return {}

    events = data.get("attachments", {}).get("events", {})
    result = {}
    for eid, ev in events.items():
        name = ev.get("name", "")
        m = _FD_NAME_RE.match(name)
        if not m:
            continue
        away = normalize_fd_name(m.group(1).strip())
        home = normalize_fd_name(m.group(2).strip())
        result[(away, home)] = int(eid)
    return result


def fetch_fd_specials(state: str, fd_event_id: int) -> dict:
    """
    Fetch FanDuel 'Specials Top/Bottom 1st' markets for a game.
    Returns dict with 'top' and 'bot' keys, each containing:
        - strikeoutLine: American odds for '1+ Strikeout'
        - impliedNSFI:   P(0 strikeouts) = 1 − P(1+ K)
        - fairNSFIOdds:  corresponding American odds string for the NSFI side
        - threeUpThreeDown: American odds for '3 Up 3 Down' (if present)
    """
    base = FD_BASE.format(state=state)
    try:
        data = api_get(base + "/event-page",
                       params={**FD_PARAMS, "includePrices": "true",
                               "priceHistory": "1", "eventId": fd_event_id})
    except RuntimeError as e:
        print(f"  [FanDuel] event-page failed (eventId={fd_event_id}): {e}")
        return {}

    markets = data.get("attachments", {}).get("markets", {})

    result = {}
    for slot, mtype_suffix in [("top", "TOP_1ST"), ("bot", "BOT_1ST")]:
        market = next(
            (m for m in markets.values()
             if mtype_suffix in m.get("marketType", "")),
            None,
        )
        if not market:
            continue

        runners_by_name = {
            r["runnerName"]: r.get("winRunnerOdds", {})
                              .get("americanDisplayOdds", {})
                              .get("americanOdds")
            for r in market.get("runners", [])
        }

        k_line = runners_by_name.get("1+ Strikeout")
        td_line = runners_by_name.get("3 Up 3 Down")

        if k_line is None:
            result[slot] = {"oddsPosted": False}
            continue

        implied_nsfi = round(1 - american_to_implied(k_line), 4)
        result[slot] = {
            "oddsPosted": True,
            "strikeoutLine": k_line,       # "1+ K" American odds (e.g. -350)
            "impliedNSFI": implied_nsfi,   # P(no strikeout this half-inning)
            "fairNSFIOdds": implied_to_american(implied_nsfi),
            "threeUpThreeDown": td_line,   # bonus context market
            "allRunners": runners_by_name, # full market for reference
        }

    return result


# ── Combined run loop ────────────────────────────────────────────────────────

def run(date_str, poll=False, interval_min=15, fd_state="il"):
    out_file = os.path.join(
        os.path.dirname(__file__), f"daily_{date_str.replace('-', '')}.json"
    )
    handedness_cache = {}

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

        # ── FanDuel odds ─────────────────────────────────────────────────────
        print("  Fetching FanDuel odds…", end=" ", flush=True)
        try:
            fd_events = fetch_fd_events(fd_state)
        except Exception as e:
            print(f"WARNING: {e}")
            fd_events = {}

        odds_found = 0
        for g in games_data:
            key = (g["awayTeam"], g["homeTeam"])
            fd_id = fd_events.get(key)
            if fd_id is None:
                # Try alias variants
                away_norm = normalize_fd_name(g["awayTeam"])
                home_norm = normalize_fd_name(g["homeTeam"])
                fd_id = fd_events.get((away_norm, home_norm))

            if fd_id:
                specials = fetch_fd_specials(fd_state, fd_id)
                g["odds"] = {"fanduel": {"eventId": fd_id, **specials}}
                if specials.get("top", {}).get("oddsPosted") or \
                   specials.get("bot", {}).get("oddsPosted"):
                    odds_found += 1
            else:
                g["odds"] = {"fanduel": None}

        print(f"{odds_found}/{len(games_data)} games have FanDuel odds.")

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

        fd = g.get("odds", {}).get("fanduel") or {}

        for slot, label in [("top", "Top 1"), ("bot", "Bot 1")]:
            half = g["topInning"] if slot == "top" else g["botInning"]
            odds_slot = fd.get(slot, {})
            pitcher = half["pitcher"]
            hand = "RHP" if pitcher["pitchHand"] == "R" else "LHP"

            lineup_str = ""
            if g["lineupComplete"]:
                lineup_str = ", ".join(
                    f"{p['name']} ({p['batSide']})" for p in half["lineup"][:3]
                ) + "…"

            if odds_slot.get("oddsPosted"):
                k_line = odds_slot["strikeoutLine"]
                k_str  = f"{k_line:+d}" if k_line > 0 else str(k_line)
                nsfi_p = odds_slot["impliedNSFI"]
                nsfi_o = odds_slot["fairNSFIOdds"]
                td_line = odds_slot.get("threeUpThreeDown")
                td_str  = f" | 3up3dn {td_line:+d}" if td_line else ""
                print(f"    {label}: {half['teamBatting']} vs {pitcher['name']} ({hand})")
                print(f"         1+K: {k_str}  →  P(NSFI)={nsfi_p:.1%}  fair odds: {nsfi_o}{td_str}")
                if lineup_str:
                    print(f"         {lineup_str}")
            else:
                print(f"    {label}: {half['teamBatting']} vs {pitcher['name']} ({hand})"
                      + ("  [odds pending]" if not odds_slot.get("oddsPosted") else ""))
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
    parser.add_argument("--state", default="il",
                        help="US state code for FanDuel API endpoint (default: il).")
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    run(date_str, poll=args.poll, interval_min=args.interval, fd_state=args.state)


if __name__ == "__main__":
    main()
