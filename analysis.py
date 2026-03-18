"""
March Madness Betting Analysis Engine
Core functions for identifying profitable trends in spread, totals, and upsets.
"""

import pandas as pd
import numpy as np
from scipy import stats


def load_data(path: str = None) -> pd.DataFrame:
    """Load tournament dataset."""
    import os
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "data", "tournament_results.csv")
    return pd.read_csv(path)


# =============================================================================
# SPREAD ANALYSIS
# =============================================================================

def spread_cover_by_seed_matchup(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze favorite cover rate by seed matchup (e.g., 1v16, 5v12)."""
    grouped = df.groupby("matchup").agg(
        games=("favorite_covered", "count"),
        fav_covers=("favorite_covered", "sum"),
        upsets=("upset", "sum"),
        avg_margin=("actual_margin", "mean"),
        avg_total=("total_points", "mean"),
        avg_spread=("estimated_spread", "mean"),
    ).reset_index()

    grouped["fav_cover_pct"] = grouped["fav_covers"] / grouped["games"]
    grouped["upset_pct"] = grouped["upsets"] / grouped["games"]
    grouped["dog_cover_pct"] = 1 - grouped["fav_cover_pct"]

    # Profitability: if you bet $100 on underdog every time at -110
    # Win pays ~$91, loss costs $100
    grouped["dog_profit_per_100"] = (
        grouped["dog_cover_pct"] * 90.91 - grouped["fav_cover_pct"] * 100
    )

    return grouped.sort_values("matchup")


def spread_cover_by_round(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze favorite cover rate by tournament round."""
    grouped = df.groupby(["round_num", "round_name"]).agg(
        games=("favorite_covered", "count"),
        fav_covers=("favorite_covered", "sum"),
        upsets=("upset", "sum"),
        avg_margin=("actual_margin", "mean"),
        avg_total=("total_points", "mean"),
    ).reset_index()

    grouped["fav_cover_pct"] = grouped["fav_covers"] / grouped["games"]
    grouped["upset_pct"] = grouped["upsets"] / grouped["games"]
    grouped["dog_cover_pct"] = 1 - grouped["fav_cover_pct"]

    return grouped.sort_values("round_num")


def spread_cover_by_seed(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze how each seed performs ATS as the favorite."""
    fav_games = df[df["seed_diff"] > 0].copy()

    grouped = fav_games.groupby("favorite_seed").agg(
        games=("favorite_covered", "count"),
        fav_covers=("favorite_covered", "sum"),
        avg_margin=("actual_margin", "mean"),
        avg_estimated_spread=("estimated_spread", "mean"),
    ).reset_index()

    grouped["cover_pct"] = grouped["fav_covers"] / grouped["games"]
    grouped["margin_vs_spread"] = grouped["avg_margin"] - grouped["avg_estimated_spread"]

    return grouped.sort_values("favorite_seed")


def spread_cover_by_spread_range(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze cover rates by estimated spread range."""
    df = df.copy()
    bins = [0, 3, 6, 10, 15, 20, 30]
    labels = ["0-3", "3-6", "6-10", "10-15", "15-20", "20+"]
    df["spread_range"] = pd.cut(df["estimated_spread"], bins=bins, labels=labels, right=False)

    grouped = df.groupby("spread_range", observed=True).agg(
        games=("favorite_covered", "count"),
        fav_covers=("favorite_covered", "sum"),
        upsets=("upset", "sum"),
        avg_margin=("actual_margin", "mean"),
        avg_total=("total_points", "mean"),
    ).reset_index()

    grouped["fav_cover_pct"] = grouped["fav_covers"] / grouped["games"]
    grouped["dog_cover_pct"] = 1 - grouped["fav_cover_pct"]
    grouped["upset_pct"] = grouped["upsets"] / grouped["games"]

    return grouped


# =============================================================================
# TOTALS ANALYSIS
# =============================================================================

def totals_by_round(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze total points scoring by round."""
    grouped = df.groupby(["round_num", "round_name"]).agg(
        games=("total_points", "count"),
        avg_total=("total_points", "mean"),
        median_total=("total_points", "median"),
        std_total=("total_points", "std"),
        min_total=("total_points", "min"),
        max_total=("total_points", "max"),
    ).reset_index()

    # Percentage of games going over common totals thresholds
    for threshold in [130, 140, 150, 160]:
        col_name = f"over_{threshold}_pct"
        over_counts = df.groupby("round_num")["total_points"].apply(
            lambda x: (x > threshold).mean()
        ).reset_index()
        over_counts.columns = ["round_num", col_name]
        grouped = grouped.merge(over_counts, on="round_num")

    return grouped.sort_values("round_num")


def totals_by_seed_matchup(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze total points by seed matchup."""
    grouped = df.groupby("matchup").agg(
        games=("total_points", "count"),
        avg_total=("total_points", "mean"),
        median_total=("total_points", "median"),
        std_total=("total_points", "std"),
    ).reset_index()

    return grouped.sort_values("avg_total", ascending=False)


def totals_trend_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze total points trends over years."""
    grouped = df.groupby("year").agg(
        games=("total_points", "count"),
        avg_total=("total_points", "mean"),
        median_total=("total_points", "median"),
        over_150_pct=("total_points", lambda x: (x > 150).mean()),
        under_130_pct=("total_points", lambda x: (x < 130).mean()),
    ).reset_index()

    return grouped


def totals_distribution(df: pd.DataFrame, round_num: int = None) -> dict:
    """Get detailed distribution statistics for total points."""
    data = df if round_num is None else df[df["round_num"] == round_num]
    totals = data["total_points"]

    result = {
        "count": len(totals),
        "mean": totals.mean(),
        "median": totals.median(),
        "std": totals.std(),
        "skewness": totals.skew(),
        "kurtosis": totals.kurtosis(),
        "percentiles": {
            "10th": totals.quantile(0.10),
            "25th": totals.quantile(0.25),
            "50th": totals.quantile(0.50),
            "75th": totals.quantile(0.75),
            "90th": totals.quantile(0.90),
        },
    }

    # Probability of hitting various over/under thresholds
    thresholds = list(range(120, 175, 5))
    result["over_probability"] = {
        t: float((totals > t).mean()) for t in thresholds
    }
    result["under_probability"] = {
        t: float((totals < t).mean()) for t in thresholds
    }

    return result


# =============================================================================
# UPSET ANALYSIS
# =============================================================================

def upset_rates_by_matchup(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze upset rates by seed matchup."""
    # Only look at games where seeds differ
    games = df[df["seed_diff"] > 0].copy()

    grouped = games.groupby("matchup").agg(
        games=("upset", "count"),
        upsets=("upset", "sum"),
        avg_margin=("actual_margin", "mean"),
    ).reset_index()

    grouped["upset_pct"] = grouped["upsets"] / grouped["games"]

    # Moneyline implied probability (rough) - favorites should win at these rates
    # If upsets happen more than market expects, there's value on underdogs
    return grouped.sort_values("upset_pct", ascending=False)


def upset_rates_by_round(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze upset rates by round."""
    games = df[df["seed_diff"] > 0].copy()

    grouped = games.groupby(["round_num", "round_name"]).agg(
        games=("upset", "count"),
        upsets=("upset", "sum"),
    ).reset_index()

    grouped["upset_pct"] = grouped["upsets"] / grouped["games"]

    return grouped.sort_values("round_num")


def cinderella_analysis(df: pd.DataFrame, min_seed: int = 9) -> pd.DataFrame:
    """Analyze how far lower-seeded teams advance (Cinderella runs)."""
    # Find max round reached by each underdog seed
    dogs = df[df["winner_seed"] >= min_seed].copy()

    runs = dogs.groupby(["year", "winner", "winner_seed"]).agg(
        max_round=("round_num", "max"),
        wins=("round_num", "count"),
    ).reset_index()

    runs = runs.sort_values(["max_round", "wins"], ascending=False)
    return runs


# =============================================================================
# PROFITABILITY ANALYSIS
# =============================================================================

def flat_bet_simulation(df: pd.DataFrame, bet_type: str = "underdog_spread",
                        unit_size: float = 100, juice: float = -110) -> pd.DataFrame:
    """Simulate flat betting on a specific strategy across all games."""
    df = df.copy()
    win_payout = unit_size * (100 / abs(juice))  # e.g., $90.91 at -110
    loss_cost = unit_size

    results = []

    if bet_type == "underdog_spread":
        for _, row in df.iterrows():
            won = not row["favorite_covered"]
            profit = win_payout if won else -loss_cost
            results.append({
                "year": row["year"],
                "round_name": row["round_name"],
                "round_num": row["round_num"],
                "matchup": row["matchup"],
                "game": f"{row['team1']} vs {row['team2']}",
                "bet": f"{row['underdog']} +{row['estimated_spread']}",
                "won": won,
                "profit": profit,
            })

    elif bet_type == "underdog_ml":
        for _, row in df.iterrows():
            won = row["upset"]
            # Rough ML payout based on seed diff
            if won:
                ml_payout = unit_size * (1 + row["seed_diff"] * 0.5)  # rough approximation
            else:
                ml_payout = 0
            profit = ml_payout - unit_size if won else -unit_size
            results.append({
                "year": row["year"],
                "round_name": row["round_name"],
                "round_num": row["round_num"],
                "matchup": row["matchup"],
                "game": f"{row['team1']} vs {row['team2']}",
                "bet": f"{row['underdog']} ML",
                "won": won,
                "profit": profit,
            })

    elif bet_type == "over":
        median_total = df["total_points"].median()
        for _, row in df.iterrows():
            won = row["total_points"] > median_total
            profit = win_payout if won else -loss_cost
            results.append({
                "year": row["year"],
                "round_name": row["round_name"],
                "round_num": row["round_num"],
                "matchup": row["matchup"],
                "game": f"{row['team1']} vs {row['team2']}",
                "bet": f"Over {median_total}",
                "won": won,
                "profit": profit,
            })

    results_df = pd.DataFrame(results)
    return results_df


def strategy_summary(sim_results: pd.DataFrame) -> dict:
    """Summarize a betting simulation."""
    return {
        "total_bets": len(sim_results),
        "wins": sim_results["won"].sum(),
        "losses": (~sim_results["won"]).sum(),
        "win_pct": sim_results["won"].mean(),
        "total_profit": sim_results["profit"].sum(),
        "avg_profit_per_bet": sim_results["profit"].mean(),
        "best_year": sim_results.groupby("year")["profit"].sum().idxmax(),
        "worst_year": sim_results.groupby("year")["profit"].sum().idxmin(),
        "profit_by_year": sim_results.groupby("year")["profit"].sum().to_dict(),
        "profit_by_round": sim_results.groupby("round_name")["profit"].sum().to_dict(),
    }


# =============================================================================
# STATISTICAL TESTS
# =============================================================================

def chi_square_test_upset_rates(df: pd.DataFrame, matchup1: str, matchup2: str) -> dict:
    """Test if upset rates significantly differ between two matchups."""
    games1 = df[df["matchup"] == matchup1]
    games2 = df[df["matchup"] == matchup2]

    table = np.array([
        [games1["upset"].sum(), len(games1) - games1["upset"].sum()],
        [games2["upset"].sum(), len(games2) - games2["upset"].sum()],
    ])

    chi2, p_value, dof, expected = stats.chi2_contingency(table)

    return {
        "matchup1": matchup1,
        "matchup2": matchup2,
        "chi2": chi2,
        "p_value": p_value,
        "significant_at_05": p_value < 0.05,
    }


def binomial_test_cover_rate(df: pd.DataFrame, subset_mask=None,
                              null_probability: float = 0.5) -> dict:
    """Test if a cover rate significantly differs from 50%."""
    data = df[subset_mask] if subset_mask is not None else df
    successes = int(data["favorite_covered"].sum())
    n = len(data)

    result = stats.binomtest(successes, n, null_probability)

    return {
        "n_games": n,
        "fav_covers": successes,
        "fav_cover_pct": successes / n,
        "p_value": result.pvalue,
        "ci_low": result.proportion_ci(confidence_level=0.95).low,
        "ci_high": result.proportion_ci(confidence_level=0.95).high,
        "significant_at_05": result.pvalue < 0.05,
    }


# =============================================================================
# KEY FINDINGS SUMMARY
# =============================================================================

def generate_key_findings(df: pd.DataFrame) -> list[str]:
    """Generate a list of key betting insights from the data."""
    findings = []

    # 1. Overall favorite cover rate
    fav_rate = df["favorite_covered"].mean()
    findings.append(
        f"Overall favorite ATS cover rate: {fav_rate:.1%} "
        f"({'favors dogs' if fav_rate < 0.5 else 'favors favorites'})"
    )

    # 2. Best underdog matchup (min 5 games for significance)
    matchup_data = spread_cover_by_seed_matchup(df)
    matchup_sig = matchup_data[matchup_data["games"] >= 5]
    if not matchup_sig.empty:
        best_dog = matchup_sig.loc[matchup_sig["dog_cover_pct"].idxmax()]
        findings.append(
            f"Best underdog matchup: {best_dog['matchup']} — dogs cover "
            f"{best_dog['dog_cover_pct']:.1%} of the time ({int(best_dog['games'])} games)"
        )

    # 3. Highest upset rate matchup (min 5 games)
    upset_data = upset_rates_by_matchup(df)
    upset_sig = upset_data[upset_data["games"] >= 5]
    if not upset_sig.empty:
        most_upsets = upset_sig.loc[upset_sig["upset_pct"].idxmax()]
        findings.append(
            f"Highest upset rate: {most_upsets['matchup']} — "
            f"{most_upsets['upset_pct']:.1%} upset rate ({int(most_upsets['upsets'])}/{int(most_upsets['games'])})"
        )

    # 4. Round with most underdog value
    round_data = spread_cover_by_round(df)
    best_dog_round = round_data.loc[round_data["dog_cover_pct"].idxmax()]
    findings.append(
        f"Best round for underdogs: {best_dog_round['round_name']} — "
        f"dogs cover {best_dog_round['dog_cover_pct']:.1%}"
    )

    # 5. Totals trend
    total_by_round = totals_by_round(df)
    first_rd = total_by_round[total_by_round["round_num"] == 1].iloc[0]
    late_rds = total_by_round[total_by_round["round_num"] >= 4]["avg_total"].mean()
    findings.append(
        f"Scoring drops in late rounds: First Round avg {first_rd['avg_total']:.1f} → "
        f"Elite 8+ avg {late_rds:.1f} total points"
    )

    # 6. 12-seed magic
    twelve_five = df[df["matchup"] == "5v12"]
    if len(twelve_five) > 0:
        twelve_upset = twelve_five["upset"].mean()
        findings.append(
            f"5 vs 12 seed upset rate: {twelve_upset:.1%} "
            f"({int(twelve_five['upset'].sum())}/{len(twelve_five)} games)"
        )

    return findings
