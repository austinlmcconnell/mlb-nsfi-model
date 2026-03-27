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


# ── DraftKings odds helpers (Playwright headless browser) ────────────────────

DK_MLB_URL = "https://sportsbook.draftkings.com/leagues/baseball/mlb"

# DraftKings team name → canonical MLB API name
DK_TEAM_ALIASES = {
    "Athletics": "Sacramento Athletics",
    "Oakland Athletics": "Sacramento Athletics",
}


def fetch_all_dk_nsfi() -> dict:
    """
    Scrape DraftKings NSFI odds using a real headless browser (Playwright).

    Strategy:
      1. Load the DK MLB page and collect all game event links.
      2. For each game, navigate to the 1st Inning > Strikeouts tab.
      3. Intercept the API responses the page makes to extract structured
         "Strikeout Thrown - 1st Inning" Yes/No odds.

    Returns dict keyed by team name:
      {team: {oddsPosted, noOdds, yesOdds, impliedNSFI, source}}
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError(
            "Playwright is not installed. Run: pip install playwright && python -m playwright install chromium"
        )

    results = {}

    print("  [DraftKings] Launching headless browser…", end=" ", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            ignore_https_errors=True,
        )
        page = context.new_page()

        # Step 1: Load MLB page and find all game event links
        try:
            page.goto(DK_MLB_URL, wait_until="domcontentloaded", timeout=30000)
            # Wait for game links to render (don't use networkidle — DK never settles)
            page.wait_for_selector('a[href*="/event/"]', timeout=15000)
            page.wait_for_timeout(2000)
        except Exception as e:
            print(f"MLB page load failed: {e}")
            browser.close()
            raise RuntimeError(f"Failed to load DraftKings MLB page: {e}")

        page_title = page.title()
        print(f"page: '{page_title}'…", end=" ", flush=True)

        # Find game event links (pattern: /event/{slug}/{eventId})
        game_links = page.eval_on_selector_all(
            'a[href*="/event/"]',
            "els => els.map(e => e.href)"
        )
        # Deduplicate and keep only unique event URLs
        seen = set()
        unique_links = []
        for link in game_links:
            # Normalize: strip query params and fragments
            base_link = link.split("?")[0].split("#")[0]
            if "/event/" in base_link and base_link not in seen:
                seen.add(base_link)
                unique_links.append(base_link)

        print(f"found {len(unique_links)} games.", flush=True)
        if unique_links:
            for ul in unique_links[:3]:
                print(f"    sample: {ul}")

        if not unique_links:
            # Try broader link search
            all_links = page.eval_on_selector_all('a', "els => els.map(e => e.href)")
            event_links = [l for l in all_links if "/event" in l.lower()]
            preview = page.inner_text("body")[:500]
            print(f"  [DraftKings] No /event/ links found.")
            print(f"    Total links on page: {len(all_links)}")
            print(f"    Links containing 'event': {len(event_links)}")
            if event_links:
                for el in event_links[:5]:
                    print(f"      {el}")
            print(f"    Page preview:\n    {preview[:300]}")
            browser.close()
            return results

        # Step 2: Visit each game's 1st Inning > Strikeouts tab
        # For the first game, log ALL response domains to find the odds API
        all_response_domains = []

        for i, game_url in enumerate(unique_links):
            strikeout_url = game_url + "?category=1st-inning&subcategory=strikeouts"
            api_responses = []
            api_urls = []

            def handle_response(response):
                url = response.url
                ct = response.headers.get("content-type", "")
                # Capture any JSON response from DK domains
                if "json" in ct and ("draftkings" in url or "dkn" in url or
                                      "sportsbook" in url or "offering" in url or
                                      "eventgroup" in url or "sbapi" in url or
                                      "sbtech" in url):
                    api_urls.append(url[:200])
                    try:
                        api_responses.append(response.json())
                    except Exception:
                        pass

            # For game 1, also log every response domain
            def debug_all_responses(response):
                if i == 0:
                    from urllib.parse import urlparse
                    domain = urlparse(response.url).netloc
                    if domain not in all_response_domains:
                        all_response_domains.append(domain)

            page.on("response", handle_response)
            page.on("response", debug_all_responses)

            try:
                page.goto(strikeout_url, wait_until="domcontentloaded", timeout=20000)
                page.wait_for_timeout(5000)  # wait for JS to render odds
            except Exception as e:
                print(f"    Game {i+1}/{len(unique_links)}: load failed ({e})")
                page.remove_listener("response", handle_response)
                page.remove_listener("response", debug_all_responses)
                continue

            # Parse intercepted API responses for strikeout markets
            game_found = 0
            for data in api_responses:
                # Handle nested structures — DK responses vary
                offer_lists = []
                # Try eventGroup > offerSubcategories path
                for sc in data.get("eventGroup", {}).get("offerSubcategories", []):
                    for og in sc.get("offerSubcategory", {}).get("offers", []):
                        for o in og:
                            offer_lists.append(o)
                # Try event > offerCategories path
                for cat in data.get("event", {}).get("offerCategories", []):
                    for sc2 in cat.get("offerSubcategoryDescriptors", []):
                        for og2 in sc2.get("offerSubcategory", {}).get("offers", []):
                            for o2 in og2:
                                offer_lists.append(o2)
                # Try flat offers array
                if "offers" in data:
                    for og in data["offers"]:
                        if isinstance(og, list):
                            offer_lists.extend(og)
                        elif isinstance(og, dict):
                            offer_lists.append(og)

                for offer in offer_lists:
                    label = offer.get("label", "").strip()
                    if "Strikeout" not in label or "1st Inning" not in label:
                        continue
                    team = label.replace("Strikeout Thrown - 1st Inning", "").strip()
                    team = DK_TEAM_ALIASES.get(team, team)
                    outcomes = {oc.get("label", "").strip(): oc
                                for oc in offer.get("outcomes", [])}
                    no_oc = outcomes.get("No")
                    yes_oc = outcomes.get("Yes")
                    if not no_oc:
                        continue
                    no_odds_str = no_oc.get("oddsAmerican")
                    yes_odds_str = yes_oc.get("oddsAmerican") if yes_oc else None
                    if not no_odds_str:
                        continue
                    no_odds = int(no_odds_str)
                    results[team] = {
                        "oddsPosted": True,
                        "noOdds": no_odds,
                        "yesOdds": int(yes_odds_str) if yes_odds_str else None,
                        "impliedNSFI": round(american_to_implied(no_odds), 4),
                        "source": "draftkings_playwright",
                    }
                    game_found += 1

            # If no API data, try scraping visible text on the page
            if game_found == 0:
                try:
                    body_text = page.inner_text("body")
                    if "Strikeout" in body_text and "1st Inning" in body_text:
                        # Look for market rows with team names and odds
                        elements = page.query_selector_all(
                            '[class*="component"], [class*="market"], [class*="offer"], '
                            '[class*="outcome-cell"], [class*="bet-button"]'
                        )
                        # Collect all visible text blocks
                        for el in elements:
                            text = el.inner_text().strip()
                            if "Strikeout Thrown - 1st Inning" in text:
                                lines = text.split("\n")
                                team_name = None
                                yes_odds = no_odds = None
                                for line in lines:
                                    line = line.strip().replace("−", "-")
                                    if "Strikeout Thrown" in line:
                                        team_name = line.replace(
                                            "Strikeout Thrown - 1st Inning", ""
                                        ).strip()
                                        team_name = DK_TEAM_ALIASES.get(team_name, team_name)
                                    elif line and (line[0] in "+-" or line[0].isdigit()):
                                        try:
                                            val = int(line.replace("+", ""))
                                            if yes_odds is None:
                                                yes_odds = val
                                            else:
                                                no_odds = val
                                        except ValueError:
                                            pass
                                if team_name and no_odds is not None:
                                    results[team_name] = {
                                        "oddsPosted": True,
                                        "noOdds": no_odds,
                                        "yesOdds": yes_odds,
                                        "impliedNSFI": round(
                                            american_to_implied(no_odds), 4
                                        ),
                                        "source": "draftkings_playwright_scrape",
                                    }
                                    game_found += 1
                except Exception:
                    pass

            status = f"{game_found} market(s)" if game_found else "no markets"
            print(f"    Game {i+1}/{len(unique_links)}: {status} "
                  f"({len(api_responses)} API resp, {len(api_urls)} API calls)")
            if not game_found and api_urls:
                for au in api_urls[:3]:
                    print(f"      API: {au}")
            if not game_found and not api_urls:
                # Show what we see on the page
                try:
                    body_preview = page.inner_text("body")[:200]
                    print(f"      Page: {body_preview}"[:120])
                except Exception:
                    pass

            page.remove_listener("response", handle_response)
            page.remove_listener("response", debug_all_responses)

            # After first game, print all response domains we saw
            if i == 0 and all_response_domains:
                print(f"    [Debug] All response domains for game 1:")
                for d in sorted(all_response_domains):
                    print(f"      {d}")

        browser.close()

    if results:
        print(f"  [DraftKings] Total: {len(results)} markets across all games.")
    else:
        print("  [DraftKings] No markets found (odds may not be posted yet).")

    return results


# ── Combined run loop ────────────────────────────────────────────────────────

def run(date_str, poll=False, interval_min=15, use_dk=True):
    out_file = os.path.join(
        os.path.dirname(__file__), f"daily_{date_str.replace('-', '')}.json"
    )
    handedness_cache = {}

    # Fetch all DraftKings NSFI odds at once via headless browser
    dk_all = {}
    if use_dk:
        print("  Fetching DraftKings odds via headless browser…", flush=True)
        try:
            dk_all = fetch_all_dk_nsfi()
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
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    run(date_str, poll=args.poll, interval_min=args.interval,
        use_dk=not args.no_dk)


if __name__ == "__main__":
    main()
