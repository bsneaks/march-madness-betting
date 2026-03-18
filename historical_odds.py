"""
Historical Odds Fetcher — Pulls real Vegas lines for past NCAA Tournaments.

Requires a paid the-odds-api.com plan ($30/month = 20k requests).
Pulls opening/closing spreads, totals, and moneylines for every tournament game.

Usage:
    python historical_odds.py           # Fetch all years (2016-2025)
    python historical_odds.py 2024      # Fetch single year
    python historical_odds.py --merge   # Merge with existing game data
"""

import requests
import pandas as pd
import numpy as np
import json
import os
import time
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

SPORT = "basketball_ncaab"
BASE_URL = "https://api.the-odds-api.com/v4"
PREFERRED_BOOKS = ["draftkings", "fanduel", "betmgm", "caesars", "bovada", "betonlineag"]


def get_api_key() -> str:
    key = os.environ.get("ODDS_API_KEY", "")
    if not key:
        key_file = os.path.join(DATA_DIR, ".odds_api_key")
        if os.path.exists(key_file):
            with open(key_file) as f:
                key = f.read().strip()
    if not key:
        raise ValueError("No API key found.")
    return key


# Tournament dates (Thursday of first round) for each year
# The API needs a date to pull historical snapshot
TOURNEY_DATES = {
    2016: ["2016-03-17", "2016-03-18", "2016-03-19", "2016-03-20",  # R1+R2
           "2016-03-24", "2016-03-25",  # S16
           "2016-03-26", "2016-03-27",  # E8
           "2016-04-02", "2016-04-04"],  # F4+Champ
    2017: ["2017-03-16", "2017-03-17", "2017-03-18", "2017-03-19",
           "2017-03-23", "2017-03-24", "2017-03-25", "2017-03-26",
           "2017-04-01", "2017-04-03"],
    2018: ["2018-03-15", "2018-03-16", "2018-03-17", "2018-03-18",
           "2018-03-22", "2018-03-23", "2018-03-24", "2018-03-25",
           "2018-03-31", "2018-04-02"],
    2019: ["2019-03-21", "2019-03-22", "2019-03-23", "2019-03-24",
           "2019-03-28", "2019-03-29", "2019-03-30", "2019-03-31",
           "2019-04-06", "2019-04-08"],
    # 2020: cancelled
    2021: ["2021-03-19", "2021-03-20", "2021-03-21", "2021-03-22",
           "2021-03-27", "2021-03-28", "2021-03-29", "2021-03-30",
           "2021-04-03", "2021-04-05"],
    2022: ["2022-03-17", "2022-03-18", "2022-03-19", "2022-03-20",
           "2022-03-24", "2022-03-25", "2022-03-26", "2022-03-27",
           "2022-04-02", "2022-04-04"],
    2023: ["2023-03-16", "2023-03-17", "2023-03-18", "2023-03-19",
           "2023-03-23", "2023-03-24", "2023-03-25", "2023-03-26",
           "2023-04-01", "2023-04-03"],
    2024: ["2024-03-21", "2024-03-22", "2024-03-23", "2024-03-24",
           "2024-03-28", "2024-03-29", "2024-03-30", "2024-03-31",
           "2024-04-06", "2024-04-08"],
    2025: ["2025-03-20", "2025-03-21", "2025-03-22", "2025-03-23",
           "2025-03-27", "2025-03-28", "2025-03-29", "2025-03-30",
           "2025-04-05", "2025-04-07"],
}


def fetch_historical_snapshot(date_str: str, api_key: str = None,
                               markets: str = "spreads,totals,h2h") -> list[dict]:
    """
    Fetch a historical odds snapshot for a given date.
    Returns list of game dicts with odds from multiple books.
    """
    if api_key is None:
        api_key = get_api_key()

    # API wants ISO 8601 format with time
    date_iso = f"{date_str}T12:00:00Z"

    url = f"{BASE_URL}/historical/sports/{SPORT}/odds"
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": markets,
        "date": date_iso,
        "oddsFormat": "american",
    }

    resp = requests.get(url, params=params, timeout=30)

    if resp.status_code == 401:
        raise PermissionError(
            "Historical odds require a paid plan. "
            "Upgrade at https://the-odds-api.com"
        )

    if resp.status_code != 200:
        print(f"  WARNING: Status {resp.status_code} for {date_str}")
        return []

    remaining = resp.headers.get("x-requests-remaining", "?")
    data = resp.json()

    # Historical endpoint wraps in {"data": [...], "timestamp": ...}
    events = data.get("data", data) if isinstance(data, dict) else data
    if not isinstance(events, list):
        events = []

    print(f"  {date_str}: {len(events)} games (remaining: {remaining})")
    return events


def parse_historical_event(event: dict) -> dict:
    """Parse a single historical event into a clean record."""
    game = {
        "event_id": event.get("id", ""),
        "commence_time": event.get("commence_time", ""),
        "home_team": event.get("home_team", ""),
        "away_team": event.get("away_team", ""),
    }

    # Collect lines from all books
    spreads_home, spreads_away = [], []
    totals_over = []
    ml_home, ml_away = [], []
    book_lines = {}

    for book in event.get("bookmakers", []):
        bk = book["key"]
        for market in book.get("markets", []):
            if market["key"] == "spreads":
                for o in market["outcomes"]:
                    pt = o.get("point", 0)
                    price = o.get("price", 0)
                    if o["name"] == event.get("home_team"):
                        spreads_home.append(pt)
                        book_lines[f"{bk}_home_spread"] = pt
                        book_lines[f"{bk}_home_spread_price"] = price
                    else:
                        spreads_away.append(pt)
                        book_lines[f"{bk}_away_spread"] = pt
                        book_lines[f"{bk}_away_spread_price"] = price

            elif market["key"] == "totals":
                for o in market["outcomes"]:
                    if o["name"] == "Over":
                        totals_over.append(o.get("point", 0))
                        book_lines[f"{bk}_over"] = o.get("point", 0)
                        book_lines[f"{bk}_over_price"] = o.get("price", 0)
                    else:
                        book_lines[f"{bk}_under"] = o.get("point", 0)
                        book_lines[f"{bk}_under_price"] = o.get("price", 0)

            elif market["key"] == "h2h":
                for o in market["outcomes"]:
                    if o["name"] == event.get("home_team"):
                        ml_home.append(o.get("price", 0))
                        book_lines[f"{bk}_home_ml"] = o.get("price", 0)
                    else:
                        ml_away.append(o.get("price", 0))
                        book_lines[f"{bk}_away_ml"] = o.get("price", 0)

    # Consensus lines
    if spreads_home:
        game["home_spread"] = round(np.mean(spreads_home), 1)
        game["away_spread"] = round(np.mean(spreads_away), 1)
    if totals_over:
        game["total_line"] = round(np.mean(totals_over), 1)
    if ml_home:
        game["home_ml"] = round(np.mean(ml_home))
        game["away_ml"] = round(np.mean(ml_away))

    # Determine favorite from spread
    if spreads_away:
        if np.mean(spreads_away) < 0:
            game["favorite"] = game["away_team"]
            game["underdog"] = game["home_team"]
            game["spread"] = round(np.mean(spreads_away), 1)
        else:
            game["favorite"] = game["home_team"]
            game["underdog"] = game["away_team"]
            game["spread"] = round(np.mean(spreads_home), 1)

    game.update(book_lines)
    return game


def fetch_tournament_year(year: int, api_key: str = None) -> pd.DataFrame:
    """Fetch all historical odds for a tournament year."""
    if api_key is None:
        api_key = get_api_key()

    dates = TOURNEY_DATES.get(year, [])
    if not dates:
        print(f"No dates configured for {year}")
        return pd.DataFrame()

    print(f"\nFetching {year} tournament odds ({len(dates)} snapshots)...")
    all_games = {}  # deduplicate by event_id

    for date_str in dates:
        events = fetch_historical_snapshot(date_str, api_key)
        for event in events:
            parsed = parse_historical_event(event)
            eid = parsed["event_id"]
            # Keep the latest snapshot (closer to game time = closing line)
            all_games[eid] = parsed
        time.sleep(0.5)  # be nice to the API

    df = pd.DataFrame(list(all_games.values()))
    if not df.empty:
        df["year"] = year
        # Save per-year file
        path = os.path.join(DATA_DIR, f"historical_odds_{year}.csv")
        df.to_csv(path, index=False)
        print(f"  Saved {len(df)} games to {path}")

    return df


def fetch_all_years(start: int = 2016, end: int = 2025,
                     api_key: str = None) -> pd.DataFrame:
    """Fetch historical odds for all tournament years."""
    if api_key is None:
        api_key = get_api_key()

    all_dfs = []
    for year in range(start, end + 1):
        if year == 2020:
            print(f"\nSkipping 2020 (cancelled)")
            continue
        df = fetch_tournament_year(year, api_key)
        if not df.empty:
            all_dfs.append(df)

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        path = os.path.join(DATA_DIR, "historical_odds_all.csv")
        combined.to_csv(path, index=False)
        print(f"\nTotal: {len(combined)} games across {len(all_dfs)} years")
        print(f"Saved to {path}")
        return combined

    return pd.DataFrame()


def merge_with_results(odds_df: pd.DataFrame = None,
                        results_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Merge historical odds with game results to create the ultimate dataset.
    Matches on team names and approximate date.
    """
    if odds_df is None:
        odds_path = os.path.join(DATA_DIR, "historical_odds_all.csv")
        if not os.path.exists(odds_path):
            print("No historical odds found. Run fetch_all_years() first.")
            return pd.DataFrame()
        odds_df = pd.read_csv(odds_path)

    if results_df is None:
        results_path = os.path.join(DATA_DIR, "tournament_results.csv")
        if not os.path.exists(results_path):
            from data_builder import build_dataset
            results_df = build_dataset()
        else:
            results_df = pd.read_csv(results_path)

    # Normalize team names for matching
    def normalize(name):
        return (str(name).lower()
                .replace("st.", "st")
                .replace("state", "st")
                .replace("'", "")
                .strip())

    odds_df["_home_norm"] = odds_df["home_team"].apply(normalize)
    odds_df["_away_norm"] = odds_df["away_team"].apply(normalize)

    merged_games = []
    unmatched = 0

    for _, result in results_df.iterrows():
        year = result["year"]
        t1_norm = normalize(result["team1"])
        t2_norm = normalize(result["team2"])

        # Find matching odds
        year_odds = odds_df[odds_df["year"] == year]
        match = year_odds[
            ((year_odds["_home_norm"].str.contains(t1_norm[:8], na=False)) &
             (year_odds["_away_norm"].str.contains(t2_norm[:8], na=False))) |
            ((year_odds["_home_norm"].str.contains(t2_norm[:8], na=False)) &
             (year_odds["_away_norm"].str.contains(t1_norm[:8], na=False)))
        ]

        row = result.to_dict()

        if len(match) > 0:
            odds_row = match.iloc[0]
            row["real_spread"] = odds_row.get("spread", np.nan)
            row["real_total_line"] = odds_row.get("total_line", np.nan)
            row["real_home_ml"] = odds_row.get("home_ml", np.nan)
            row["real_away_ml"] = odds_row.get("away_ml", np.nan)
            row["real_favorite"] = odds_row.get("favorite", "")
            row["real_underdog"] = odds_row.get("underdog", "")

            # Calculate real ATS result
            if pd.notna(row["real_spread"]):
                real_spread_abs = abs(row["real_spread"])
                # Did the favorite cover the real spread?
                row["real_fav_covered"] = row["actual_margin"] > real_spread_abs
                row["real_dog_covered"] = not row["real_fav_covered"]

            # Calculate real over/under result
            if pd.notna(row["real_total_line"]):
                row["went_over"] = row["total_points"] > row["real_total_line"]
                row["went_under"] = row["total_points"] < row["real_total_line"]
                row["total_push"] = row["total_points"] == row["real_total_line"]
        else:
            unmatched += 1
            row["real_spread"] = np.nan
            row["real_total_line"] = np.nan

        merged_games.append(row)

    merged_df = pd.DataFrame(merged_games)
    matched = merged_df["real_spread"].notna().sum()
    print(f"Merged: {matched} matched, {unmatched} unmatched out of {len(results_df)} games")

    # Save
    path = os.path.join(DATA_DIR, "tournament_complete.csv")
    merged_df.to_csv(path, index=False)
    print(f"Saved complete dataset to {path}")

    return merged_df


def print_real_vs_estimated(merged_df: pd.DataFrame):
    """Compare real lines to seed-estimated lines."""
    has_real = merged_df[merged_df["real_spread"].notna()]

    if len(has_real) == 0:
        print("No games with real lines found.")
        return

    print(f"\n{'='*60}")
    print(f"REAL VEGAS LINES vs SEED-BASED ESTIMATES ({len(has_real)} games)")
    print(f"{'='*60}")

    # ATS comparison
    est_dog_cover = 1 - has_real["favorite_covered"].mean()
    real_dog_cover = has_real["real_dog_covered"].mean()
    print(f"\nUnderdog ATS cover rate:")
    print(f"  Estimated spreads: {est_dog_cover:.1%}")
    print(f"  Real Vegas lines:  {real_dog_cover:.1%}")

    # Totals comparison
    if "went_over" in has_real.columns:
        over_rate = has_real["went_over"].mean()
        under_rate = has_real["went_under"].mean()
        print(f"\nOver/Under rates (real lines):")
        print(f"  Over:  {over_rate:.1%}")
        print(f"  Under: {under_rate:.1%}")

    # Average spread comparison
    print(f"\nAverage spread:")
    print(f"  Estimated: {has_real['estimated_spread'].mean():.1f}")
    print(f"  Real:      {has_real['real_spread'].abs().mean():.1f}")

    # Average total line
    if "real_total_line" in has_real.columns:
        print(f"\nAverage total line:")
        print(f"  Actual scoring: {has_real['total_points'].mean():.1f}")
        print(f"  Real line:      {has_real['real_total_line'].mean():.1f}")


if __name__ == "__main__":
    import sys

    api_key = get_api_key()

    if len(sys.argv) > 1:
        if sys.argv[1] == "--merge":
            merged = merge_with_results()
            if not merged.empty:
                print_real_vs_estimated(merged)
        else:
            year = int(sys.argv[1])
            fetch_tournament_year(year, api_key)
    else:
        print("Fetching historical odds for all tournament years...")
        print("This will use ~90 API requests (9 years x 10 snapshots).\n")
        df = fetch_all_years(api_key=api_key)
        if not df.empty:
            print("\nMerging with game results...")
            merged = merge_with_results(df)
            print_real_vs_estimated(merged)
