"""
Live Odds Integration via the-odds-api.com

Free tier: 500 requests/month — more than enough for tournament betting.
Sign up at: https://the-odds-api.com/ to get your API key.

Pulls:
- Current spreads (point spreads from multiple sportsbooks)
- Current totals (over/under lines)
- Moneylines
"""

import requests
import pandas as pd
import json
import os
from datetime import datetime, timezone

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# NCAA Basketball tournament
SPORT = "basketball_ncaab"
BASE_URL = "https://api.the-odds-api.com/v4/sports"

# Popular books to compare lines
PREFERRED_BOOKS = [
    "draftkings", "fanduel", "betmgm", "caesars",
    "pointsbetus", "bovada", "betonlineag"
]


def get_api_key() -> str:
    """Get API key from environment or prompt."""
    key = os.environ.get("ODDS_API_KEY", "")
    if not key:
        key_file = os.path.join(DATA_DIR, ".odds_api_key")
        if os.path.exists(key_file):
            with open(key_file) as f:
                key = f.read().strip()
    if not key:
        raise ValueError(
            "No API key found. Set ODDS_API_KEY env var or save key to data/.odds_api_key\n"
            "Get a free key at: https://the-odds-api.com/"
        )
    return key


def save_api_key(key: str):
    """Save API key locally for convenience."""
    key_file = os.path.join(DATA_DIR, ".odds_api_key")
    with open(key_file, "w") as f:
        f.write(key)
    print(f"API key saved to {key_file}")


def check_usage(api_key: str = None) -> dict:
    """Check API usage/remaining requests."""
    if api_key is None:
        api_key = get_api_key()
    resp = requests.get(
        f"{BASE_URL}",
        params={"apiKey": api_key},
        timeout=15,
    )
    return {
        "requests_remaining": resp.headers.get("x-requests-remaining", "unknown"),
        "requests_used": resp.headers.get("x-requests-used", "unknown"),
    }


def fetch_live_odds(api_key: str = None, markets: str = "spreads,totals,h2h",
                    regions: str = "us") -> pd.DataFrame:
    """
    Fetch current NCAAB odds from the-odds-api.

    Returns DataFrame with one row per game, columns for each book's lines.
    """
    if api_key is None:
        api_key = get_api_key()

    url = f"{BASE_URL}/{SPORT}/odds"
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": "american",
    }

    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"API error {resp.status_code}: {resp.text}")

    remaining = resp.headers.get("x-requests-remaining", "?")
    print(f"API requests remaining: {remaining}")

    data = resp.json()
    if not data:
        print("No games found. The tournament may not have started or games may be completed.")
        return pd.DataFrame()

    return parse_odds_response(data)


def parse_odds_response(data: list[dict]) -> pd.DataFrame:
    """Parse the-odds-api response into a clean DataFrame."""
    games = []

    for event in data:
        game = {
            "game_id": event["id"],
            "sport": event.get("sport_title", ""),
            "commence_time": event["commence_time"],
            "commence_dt": datetime.fromisoformat(
                event["commence_time"].replace("Z", "+00:00")
            ).astimezone(),
            "home_team": event["home_team"],
            "away_team": event["away_team"],
        }

        # Parse each bookmaker's odds
        spreads = {}
        totals = {}
        moneylines = {}

        for book in event.get("bookmakers", []):
            book_key = book["key"]
            book_title = book["title"]

            for market in book.get("markets", []):
                if market["key"] == "spreads":
                    for outcome in market["outcomes"]:
                        side = "home" if outcome["name"] == event["home_team"] else "away"
                        spreads[f"{book_key}_{side}_spread"] = outcome.get("point", 0)
                        spreads[f"{book_key}_{side}_spread_price"] = outcome.get("price", 0)

                elif market["key"] == "totals":
                    for outcome in market["outcomes"]:
                        if outcome["name"] == "Over":
                            totals[f"{book_key}_over"] = outcome.get("point", 0)
                            totals[f"{book_key}_over_price"] = outcome.get("price", 0)
                        elif outcome["name"] == "Under":
                            totals[f"{book_key}_under"] = outcome.get("point", 0)
                            totals[f"{book_key}_under_price"] = outcome.get("price", 0)

                elif market["key"] == "h2h":
                    for outcome in market["outcomes"]:
                        side = "home" if outcome["name"] == event["home_team"] else "away"
                        moneylines[f"{book_key}_{side}_ml"] = outcome.get("price", 0)

        game.update(spreads)
        game.update(totals)
        game.update(moneylines)
        games.append(game)

    df = pd.DataFrame(games)

    # Add consensus/average lines
    if not df.empty:
        df = add_consensus_lines(df)

    return df


def add_consensus_lines(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate consensus (average) lines across books."""
    # Find all spread columns for away team
    away_spread_cols = [c for c in df.columns
                        if c.endswith("_away_spread") and not c.endswith("_price")]
    home_spread_cols = [c for c in df.columns
                        if c.endswith("_home_spread") and not c.endswith("_price")]
    over_cols = [c for c in df.columns
                 if c.endswith("_over") and not c.endswith("_price")]
    under_cols = [c for c in df.columns
                  if c.endswith("_under") and not c.endswith("_price")]
    home_ml_cols = [c for c in df.columns if c.endswith("_home_ml")]
    away_ml_cols = [c for c in df.columns if c.endswith("_away_ml")]

    if away_spread_cols:
        df["consensus_away_spread"] = df[away_spread_cols].mean(axis=1)
    if home_spread_cols:
        df["consensus_home_spread"] = df[home_spread_cols].mean(axis=1)
    if over_cols:
        df["consensus_total"] = df[over_cols].mean(axis=1)
    if home_ml_cols:
        df["consensus_home_ml"] = df[home_ml_cols].mean(axis=1)
    if away_ml_cols:
        df["consensus_away_ml"] = df[away_ml_cols].mean(axis=1)

    return df


def get_tournament_games(api_key: str = None) -> pd.DataFrame:
    """
    Fetch and format current March Madness games with clean summary.
    Returns a simplified DataFrame ready for analysis.
    """
    raw = fetch_live_odds(api_key)
    if raw.empty:
        return raw

    # Build clean summary
    summary = []
    for _, row in raw.iterrows():
        game = {
            "game_time": row["commence_dt"].strftime("%a %m/%d %I:%M%p"),
            "away_team": row["away_team"],
            "home_team": row["home_team"],
        }

        # Consensus spread (from away team perspective)
        if "consensus_away_spread" in row:
            spread = row["consensus_away_spread"]
            if spread > 0:
                game["favorite"] = row["home_team"]
                game["spread"] = -abs(spread)
            else:
                game["favorite"] = row["away_team"]
                game["spread"] = spread
            game["underdog"] = (row["home_team"]
                                if game["favorite"] == row["away_team"]
                                else row["away_team"])

        # Consensus total
        if "consensus_total" in row:
            game["total"] = round(row["consensus_total"], 1)

        # Consensus moneylines
        if "consensus_home_ml" in row:
            game["home_ml"] = round(row.get("consensus_home_ml", 0))
            game["away_ml"] = round(row.get("consensus_away_ml", 0))

        # Best available lines (line shopping)
        away_spread_cols = [c for c in raw.columns
                            if c.endswith("_away_spread") and not c.endswith("_price")]
        if away_spread_cols:
            # Best spread for away team (most points)
            game["best_away_spread"] = row[away_spread_cols].max()
            game["best_away_spread_book"] = row[away_spread_cols].idxmax().split("_")[0]
            # Best spread for home team
            home_spread_cols = [c for c in raw.columns
                                if c.endswith("_home_spread") and not c.endswith("_price")]
            if home_spread_cols:
                game["best_home_spread"] = row[home_spread_cols].max()
                game["best_home_spread_book"] = row[home_spread_cols].idxmax().split("_")[0]

        summary.append(game)

    result = pd.DataFrame(summary)

    # Save to CSV
    output_path = os.path.join(DATA_DIR, "live_odds.csv")
    result.to_csv(output_path, index=False)
    print(f"Saved {len(result)} games to {output_path}")

    # Also save raw data
    raw_path = os.path.join(DATA_DIR, "live_odds_raw.csv")
    raw.to_csv(raw_path, index=False)

    return result


def compare_books(api_key: str = None) -> pd.DataFrame:
    """
    Show line comparison across sportsbooks for each game.
    Useful for finding the best number.
    """
    raw = fetch_live_odds(api_key)
    if raw.empty:
        return raw

    comparisons = []
    for _, row in raw.iterrows():
        matchup = f"{row['away_team']} @ {row['home_team']}"

        for book_key in PREFERRED_BOOKS:
            entry = {"matchup": matchup, "book": book_key}

            # Spread
            away_sp = f"{book_key}_away_spread"
            home_sp = f"{book_key}_home_spread"
            if away_sp in row and pd.notna(row[away_sp]):
                entry["away_spread"] = row[away_sp]
            if home_sp in row and pd.notna(row[home_sp]):
                entry["home_spread"] = row[home_sp]

            # Total
            over_col = f"{book_key}_over"
            if over_col in row and pd.notna(row[over_col]):
                entry["total"] = row[over_col]

            # Moneyline
            home_ml = f"{book_key}_home_ml"
            away_ml = f"{book_key}_away_ml"
            if home_ml in row and pd.notna(row[home_ml]):
                entry["home_ml"] = row[home_ml]
            if away_ml in row and pd.notna(row[away_ml]):
                entry["away_ml"] = row[away_ml]

            if len(entry) > 2:  # has data beyond matchup + book
                comparisons.append(entry)

    return pd.DataFrame(comparisons)


def implied_probability(american_odds: float) -> float:
    """Convert American odds to implied probability."""
    if american_odds > 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)


def find_value_bets(live_odds: pd.DataFrame, historical_df: pd.DataFrame,
                    min_edge: float = 0.05) -> list[dict]:
    """
    Compare live lines to historical performance to find potential value.

    min_edge: minimum edge over implied probability to flag (0.05 = 5%)
    """
    value_bets = []

    # Historical cover rates by spread range
    hist = historical_df.copy()

    for _, game in live_odds.iterrows():
        if "spread" not in game or "total" not in game:
            continue

        spread = abs(game.get("spread", 0))

        # Find historical games with similar spread
        margin = 2.0
        similar = hist[
            (hist["estimated_spread"] >= spread - margin) &
            (hist["estimated_spread"] <= spread + margin)
        ]

        if len(similar) < 5:
            continue

        # Historical dog cover rate for this spread range
        dog_cover_rate = 1 - similar["favorite_covered"].mean()

        # Historical over rate for this total range
        total_line = game.get("total", 140)
        over_rate = (similar["total_points"] > total_line).mean()
        under_rate = 1 - over_rate

        # Check for spread value
        if dog_cover_rate > 0.524 + min_edge:  # 52.4% is breakeven at -110
            value_bets.append({
                "game": f"{game.get('away_team', '?')} vs {game.get('home_team', '?')}",
                "bet_type": "Underdog ATS",
                "team": game.get("underdog", "?"),
                "line": f"+{spread}",
                "historical_cover_rate": f"{dog_cover_rate:.1%}",
                "sample_size": len(similar),
                "edge": f"{dog_cover_rate - 0.524:.1%}",
                "confidence": "HIGH" if len(similar) >= 20 else "MEDIUM",
            })

        # Check for totals value
        if over_rate > 0.524 + min_edge:
            value_bets.append({
                "game": f"{game.get('away_team', '?')} vs {game.get('home_team', '?')}",
                "bet_type": "Over",
                "team": "-",
                "line": f"O {total_line}",
                "historical_cover_rate": f"{over_rate:.1%}",
                "sample_size": len(similar),
                "edge": f"{over_rate - 0.524:.1%}",
                "confidence": "HIGH" if len(similar) >= 20 else "MEDIUM",
            })

        if under_rate > 0.524 + min_edge:
            value_bets.append({
                "game": f"{game.get('away_team', '?')} vs {game.get('home_team', '?')}",
                "bet_type": "Under",
                "team": "-",
                "line": f"U {total_line}",
                "historical_cover_rate": f"{under_rate:.1%}",
                "sample_size": len(similar),
                "edge": f"{under_rate - 0.524:.1%}",
                "confidence": "HIGH" if len(similar) >= 20 else "MEDIUM",
            })

    return value_bets


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "save-key":
        key = input("Enter your the-odds-api.com API key: ").strip()
        save_api_key(key)
    else:
        try:
            print("Fetching live NCAA basketball odds...\n")
            games = get_tournament_games()
            if not games.empty:
                print("\nCurrent Games:")
                print(games.to_string(index=False))

                usage = check_usage()
                print(f"\nAPI requests remaining: {usage['requests_remaining']}")
            else:
                print("No games currently available.")
        except ValueError as e:
            print(f"Setup needed: {e}")
