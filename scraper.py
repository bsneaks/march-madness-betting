"""
March Madness Historical Data Scraper
Pulls tournament results, seeds, and game data from Sports Reference (2016-2025).
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import json
import re
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) MarchMadnessResearch/1.0"
}

# Sports Reference base URL for NCAA tournament
SR_BASE = "https://www.sports-reference.com/cbb/postseason"


def scrape_tournament_year(year: int) -> list[dict]:
    """Scrape NCAA tournament results for a given year from Sports Reference."""
    url = f"{SR_BASE}/men/{year}-ncaa.html"
    print(f"  Fetching {year} tournament data...")

    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"  WARNING: Got status {resp.status_code} for {year}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    games = []

    # Parse the bracket - Sports Reference uses a specific structure
    # Look for game entries in the bracket
    bracket = soup.find("div", {"id": "bracket"})
    if not bracket:
        bracket = soup.find("div", class_="bracket")
    if not bracket:
        print(f"  WARNING: Could not find bracket for {year}")
        return []

    # Each round is in a div; games are in pairs of teams
    rounds_map = {
        1: "First Round",
        2: "Second Round",
        3: "Sweet 16",
        4: "Elite 8",
        5: "Final Four",
        6: "Championship",
    }

    # Parse all game links from the bracket
    round_divs = bracket.find_all("div", class_=re.compile(r"round"))
    for round_div in round_divs:
        round_links = round_div.find_all("a")
        for link in round_links:
            href = link.get("href", "")
            if "/boxscores/" in href:
                game_info = parse_bracket_game(round_div, link, year)
                if game_info:
                    games.append(game_info)

    # Alternative parsing: look for all game pairs directly
    if not games:
        games = parse_bracket_structure(bracket, year)

    return games


def parse_bracket_structure(bracket, year: int) -> list[dict]:
    """Parse the bracket structure to extract game results."""
    games = []
    # Sports Reference bracket has team entries with seeds and scores
    # Find all span elements with seeds and team names
    all_text = bracket.get_text(separator="\n")
    lines = [l.strip() for l in all_text.split("\n") if l.strip()]

    # Pattern: seed, team name, score appear in sequence for each game
    i = 0
    game_num = 0
    round_num = 0
    current_round_games = []

    while i < len(lines) - 3:
        # Try to match a game: seed1 team1 score1 seed2 team2 score2
        seed_match1 = re.match(r"^(\d{1,2})$", lines[i])
        if seed_match1:
            seed1 = int(seed_match1.group(1))
            team1 = lines[i + 1] if i + 1 < len(lines) else ""
            score_match1 = re.match(r"^(\d{2,3})$", lines[i + 2]) if i + 2 < len(lines) else None

            if score_match1 and i + 5 < len(lines):
                score1 = int(score_match1.group(1))
                seed_match2 = re.match(r"^(\d{1,2})$", lines[i + 3])

                if seed_match2:
                    seed2 = int(seed_match2.group(1))
                    team2 = lines[i + 4]
                    score_match2 = re.match(r"^(\d{2,3})$", lines[i + 5])

                    if score_match2:
                        score2 = int(score_match2.group(1))
                        game_num += 1
                        games.append({
                            "year": year,
                            "game_num": game_num,
                            "team1": team1,
                            "seed1": seed1,
                            "score1": score1,
                            "team2": team2,
                            "seed2": seed2,
                            "score2": score2,
                            "total_points": score1 + score2,
                            "spread": abs(score1 - score2),
                            "winner": team1 if score1 > score2 else team2,
                            "winner_seed": seed1 if score1 > score2 else seed2,
                            "loser": team2 if score1 > score2 else team1,
                            "loser_seed": seed2 if score1 > score2 else seed1,
                            "upset": (seed1 > seed2 and score1 > score2) or (seed2 > seed1 and score2 > score1),
                        })
                        i += 6
                        continue
        i += 1

    # Assign rounds based on game count (63 total games in tournament)
    assign_rounds(games)
    return games


def parse_bracket_game(round_div, link, year: int) -> dict | None:
    """Parse a single game from a bracket link."""
    # This is a fallback parser for individual game links
    return None


def assign_rounds(games: list[dict]):
    """Assign round numbers based on position in bracket."""
    # Standard tournament: 32 + 16 + 8 + 4 + 2 + 1 = 63 games
    round_sizes = [32, 16, 8, 4, 2, 1]
    round_names = ["First Round", "Second Round", "Sweet 16", "Elite 8", "Final Four", "Championship"]

    idx = 0
    for round_num, (size, name) in enumerate(zip(round_sizes, round_names), 1):
        for g in range(size):
            if idx < len(games):
                games[idx]["round_num"] = round_num
                games[idx]["round_name"] = name
                idx += 1


def scrape_all_years(start_year: int = 2016, end_year: int = 2025) -> pd.DataFrame:
    """Scrape tournament data for a range of years."""
    all_games = []

    for year in range(start_year, end_year + 1):
        if year == 2020:
            print(f"  Skipping 2020 (tournament cancelled due to COVID)")
            continue

        games = scrape_tournament_year(year)
        all_games.extend(games)
        print(f"  Found {len(games)} games for {year}")
        time.sleep(3)  # Be respectful to the server

    df = pd.DataFrame(all_games)
    if not df.empty:
        output_path = os.path.join(DATA_DIR, "tournament_results.csv")
        df.to_csv(output_path, index=False)
        print(f"\nSaved {len(df)} total games to {output_path}")
    return df


def load_or_scrape(force_refresh: bool = False) -> pd.DataFrame:
    """Load cached data or scrape fresh."""
    cache_path = os.path.join(DATA_DIR, "tournament_results.csv")
    if os.path.exists(cache_path) and not force_refresh:
        print("Loading cached tournament data...")
        return pd.read_csv(cache_path)

    print("Scraping tournament data (this takes ~30 seconds)...")
    return scrape_all_years()


if __name__ == "__main__":
    df = scrape_all_years()
    if not df.empty:
        print(f"\nDataset shape: {df.shape}")
        print(f"Years covered: {sorted(df['year'].unique())}")
        print(f"Games per year:\n{df.groupby('year').size()}")
    else:
        print("No data scraped. Check the parsing logic or try the manual data loader.")
