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


# ── DraftKings odds helpers ──────────────────────────────────────────────────

DK_BASE = "https://sportsbook.draftkings.com/sites/US-SB/api/v5"
DK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://sportsbook.draftkings.com/leagues/baseball/mlb",
    "Origin": "https://sportsbook.draftkings.com",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
}

# Persistent session to carry cookies across DK requests
_dk_session = None

def _get_dk_session():
    """Return a requests.Session that has visited the DK sportsbook page (sets cookies)."""
    global _dk_session
    if _dk_session is not None:
        return _dk_session
    _dk_session = requests.Session()
    _dk_session.headers.update(DK_HEADERS)
    try:
        _dk_session.get("https://sportsbook.draftkings.com/leagues/baseball/mlb",
                        timeout=15, headers={
                            "User-Agent": DK_HEADERS["User-Agent"],
                            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                            "Accept-Language": "en-US,en;q=0.9",
                        })
    except Exception:
        pass
    return _dk_session

# DraftKings team name → canonical MLB API name
DK_TEAM_ALIASES = {
    "Athletics": "Sacramento Athletics",
    "Oakland Athletics": "Sacramento Athletics",
}

# MLB event group IDs on DraftKings
_DK_GROUP_ID = 84240   # MLB regular season


def _dk_get_curl(url, retries=2):
    """
    GET from DraftKings using system curl (bypasses Python TLS fingerprinting).
    Falls back to requests.Session if curl is unavailable.
    """
    import subprocess, shutil
    curl_path = shutil.which("curl")
    if curl_path:
        for attempt in range(retries):
            try:
                result = subprocess.run([
                    curl_path, "-s", "-L",
                    "-H", f"User-Agent: {DK_HEADERS['User-Agent']}",
                    "-H", "Accept: application/json, text/plain, */*",
                    "-H", "Accept-Language: en-US,en;q=0.9",
                    "-H", "Referer: https://sportsbook.draftkings.com/leagues/baseball/mlb",
                    "-H", "Origin: https://sportsbook.draftkings.com",
                    "--max-time", "15",
                    url,
                ], capture_output=True, text=True, timeout=20)
                if result.returncode != 0:
                    if attempt < retries - 1:
                        time.sleep(2)
                        continue
                    raise RuntimeError(f"curl failed: {result.stderr[:200]}")
                body = result.stdout.strip()
                if not body or body.startswith("<!") or "<html" in body[:200].lower():
                    raise RuntimeError("DraftKings returned HTML (likely blocked)")
                return json.loads(body)
            except json.JSONDecodeError:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                raise RuntimeError(f"DraftKings returned non-JSON: {body[:100]}")
            except subprocess.TimeoutExpired:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                raise RuntimeError("curl timed out")
    # Fallback to requests session
    session = _get_dk_session()
    r = session.get(url, timeout=15)
    if r.status_code == 403:
        raise RuntimeError("DraftKings returned 403 — blocked by TLS fingerprinting.")
    r.raise_for_status()
    return r.json()


def _dk_get(path, params=None, retries=2):
    """GET from DraftKings API; raises RuntimeError on failure or non-JSON response."""
    url = DK_BASE + path
    if params:
        from urllib.parse import urlencode
        url += "?" + urlencode(params)
    return _dk_get_curl(url, retries=retries)


def fetch_dk_categories() -> tuple[int | None, int | None]:
    """
    Return (inning_props_category_id, strikeouts_subcategory_id) from the
    MLB event group. Returns (None, None) if the structure can't be found.
    """
    try:
        data = _dk_get(f"/eventgroups/{_DK_GROUP_ID}", params={"includeOnly": "offerCategories"})
    except RuntimeError as e:
        print(f"  [DraftKings] {e}")
        return None, None

    cats = data.get("eventGroup", {}).get("offerCategories", [])
    # Look for "Inning Props", "Game Props", or "1st Inning" category
    inning_cat = next(
        (c for c in cats if any(kw in c.get("name", "").lower()
                                for kw in ["inning prop", "1st inning", "inning lines"])),
        None,
    )
    if not inning_cat:
        # Fall back to any category whose subcategories mention strikeout
        for c in cats:
            for sub in c.get("offerSubcategoryDescriptors", []):
                if "strikeout" in sub.get("name", "").lower():
                    inning_cat = c
                    break
    if not inning_cat:
        return None, None

    cat_id = inning_cat["id"]
    strikeout_sub = next(
        (s for s in inning_cat.get("offerSubcategoryDescriptors", [])
         if "strikeout" in s.get("name", "").lower()),
        None,
    )
    sub_id = strikeout_sub["id"] if strikeout_sub else None
    return cat_id, sub_id


def fetch_dk_nsfi(home_team: str, away_team: str,
                  cat_id: int, sub_id: int | None) -> dict:
    """
    Fetch DraftKings '{TEAM} Strikeout Thrown - 1st Inning' Yes/No markets
    for a specific game. Returns dict with 'top'/'bot' keys, each containing:
        - noOdds:       American odds for 'No' outcome (= NSFI side)
        - yesOdds:      American odds for 'Yes' outcome
        - impliedNSFI:  P(No strikeout)
        - source:       'draftkings_direct'
    """
    params = {}
    if sub_id:
        params["subcategoryId"] = sub_id

    try:
        data = _dk_get(f"/eventgroups/{_DK_GROUP_ID}/categories/{cat_id}", params=params)
    except RuntimeError as e:
        print(f"  [DraftKings] {e}")
        return {}

    # Navigate: data → eventGroup → offerSubcategories → subcategory.offers
    subcats = data.get("eventGroup", {}).get("offerSubcategories", [])
    offers_all = []
    for sc in subcats:
        for offer in sc.get("offerSubcategory", {}).get("offers", []):
            for o in offer:
                offers_all.append(o)

    # Match offers: "{TEAM} Strikeout Thrown - 1st Inning"
    result = {}
    for slot, team in [("top", away_team), ("bot", home_team)]:
        # DraftKings may use aliases
        dk_team = DK_TEAM_ALIASES.get(team, team)
        target_name = f"{dk_team} Strikeout Thrown - 1st Inning"

        offer = next(
            (o for o in offers_all
             if o.get("label", "").strip().lower() == target_name.lower()),
            None,
        )
        if not offer:
            result[slot] = {"oddsPosted": False}
            continue

        outcomes = {oc.get("label", "").strip(): oc for oc in offer.get("outcomes", [])}
        no_oc  = outcomes.get("No")
        yes_oc = outcomes.get("Yes")

        def _dk_odds(oc):
            if not oc:
                return None
            return oc.get("oddsAmerican")  # string like "-115" or "+105"

        no_odds_str  = _dk_odds(no_oc)
        yes_odds_str = _dk_odds(yes_oc)

        if no_odds_str is None:
            result[slot] = {"oddsPosted": False}
            continue

        no_odds = int(no_odds_str)
        implied_nsfi = round(american_to_implied(no_odds), 4)

        result[slot] = {
            "oddsPosted": True,
            "noOdds": no_odds,            # Direct NSFI price (e.g. +115)
            "yesOdds": int(yes_odds_str) if yes_odds_str else None,
            "impliedNSFI": implied_nsfi,
            "source": "draftkings_direct",
        }

    return result


# ── Combined run loop ────────────────────────────────────────────────────────

def run(date_str, poll=False, interval_min=15, fd_state="il", use_dk=True):
    out_file = os.path.join(
        os.path.dirname(__file__), f"daily_{date_str.replace('-', '')}.json"
    )
    handedness_cache = {}

    # Discover DraftKings category/subcategory IDs once per session
    dk_cat_id, dk_sub_id = None, None
    if use_dk:
        print("  Discovering DraftKings market structure…", end=" ", flush=True)
        try:
            dk_cat_id, dk_sub_id = fetch_dk_categories()
            if dk_cat_id:
                print(f"found (cat={dk_cat_id}, sub={dk_sub_id})")
            else:
                print("not available (will use FanDuel derived odds)")
                use_dk = False
        except Exception as e:
            print(f"skipped ({e})")
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
                away_norm = normalize_fd_name(g["awayTeam"])
                home_norm = normalize_fd_name(g["homeTeam"])
                fd_id = fd_events.get((away_norm, home_norm))

            fd_specials = {}
            if fd_id:
                fd_specials = fetch_fd_specials(fd_state, fd_id)

            # DraftKings: direct NSFI Yes/No odds
            dk_specials = {}
            if use_dk and dk_cat_id:
                dk_specials = fetch_dk_nsfi(
                    g["homeTeam"], g["awayTeam"], dk_cat_id, dk_sub_id
                )

            g["odds"] = {
                "fanduel": {"eventId": fd_id, **fd_specials} if fd_id else None,
                "draftkings": dk_specials or None,
            }

            # Count game as having odds if either book has the NSFI line
            fd_ok = fd_specials.get("top", {}).get("oddsPosted") or \
                    fd_specials.get("bot", {}).get("oddsPosted")
            dk_ok = dk_specials.get("top", {}).get("oddsPosted") or \
                    dk_specials.get("bot", {}).get("oddsPosted")
            if fd_ok or dk_ok:
                odds_found += 1

        dk_label = f" | DraftKings: {sum(1 for g in games_data if g['odds'].get('draftkings'))}" if use_dk else ""
        print(f"{odds_found}/{len(games_data)} games have odds.{dk_label}")

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

            dk_slot = (g.get("odds", {}).get("draftkings") or {}).get(slot, {})

            print(f"    {label}: {half['teamBatting']} vs {pitcher['name']} ({hand})")

            # DraftKings direct NSFI line (preferred)
            if dk_slot.get("oddsPosted"):
                no_odds = dk_slot["noOdds"]
                yes_odds = dk_slot.get("yesOdds")
                nsfi_p = dk_slot["impliedNSFI"]
                no_str = f"+{no_odds}" if no_odds > 0 else str(no_odds)
                yes_str = (f"+{yes_odds}" if yes_odds and yes_odds > 0 else str(yes_odds)) if yes_odds else "?"
                print(f"         DK NSFI No: {no_str}  Yes: {yes_str}  P(NSFI)={nsfi_p:.1%}")
            # FanDuel derived NSFI (fallback)
            elif odds_slot.get("oddsPosted"):
                k_line = odds_slot["strikeoutLine"]
                k_str  = f"{k_line:+d}" if k_line > 0 else str(k_line)
                nsfi_p = odds_slot["impliedNSFI"]
                nsfi_o = odds_slot["fairNSFIOdds"]
                td_line = odds_slot.get("threeUpThreeDown")
                td_str  = f" | 3up3dn {td_line:+d}" if td_line else ""
                print(f"         FD 1+K: {k_str}  →  P(NSFI)={nsfi_p:.1%}  fair: {nsfi_o}{td_str}")
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
    parser.add_argument("--state", default="il",
                        help="US state code for FanDuel API endpoint (default: il).")
    parser.add_argument("--no-dk", action="store_true",
                        help="Skip DraftKings odds (use FanDuel derived odds only).")
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    run(date_str, poll=args.poll, interval_min=args.interval,
        fd_state=args.state, use_dk=not args.no_dk)


if __name__ == "__main__":
    main()
