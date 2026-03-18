"""
03_fundamental_scoring.py -- Multi-Criteria Scoring for BRVM Stocks
Author: Kouame Ruben
Description: Scores each stock 0-100 based on fundamental + technical criteria.
             Generates BUY/HOLD/WATCH/SELL signals.
"""

import pandas as pd
import numpy as np
from pathlib import Path


def normalize_score(values, lower_is_better=False):
    """Normalize values to 0-100 scale."""
    if lower_is_better:
        values = -values
    rng = values.max() - values.min()
    if rng == 0:
        return pd.Series(50, index=values.index)
    return ((values - values.min()) / rng * 100).round(0).astype(int)


def compute_composite_score(df):
    """Compute weighted composite score from fundamental and technical indicators."""
    df = df.copy()
    
    # Individual scores (0-100)
    df["score_div_yield"] = normalize_score(df["div_yield_pct"])
    df["score_pe"]        = normalize_score(df["pe_ratio"], lower_is_better=True)
    df["score_roe"]       = normalize_score(df["roe_pct"])
    df["score_pb"]        = normalize_score(df["pb_ratio"], lower_is_better=True)
    df["score_rsi"]       = normalize_score((df["rsi_14"] - 50).abs(), lower_is_better=True)
    df["score_ma"]        = np.where(df["ma_signal"] == "above", 70, 30)
    
    # Weighted composite
    weights = {
        "div_yield": 0.25,
        "pe":        0.20,
        "roe":       0.20,
        "pb":        0.10,
        "rsi":       0.15,
        "ma":        0.10,
    }
    
    df["composite_score"] = (
        df["score_div_yield"] * weights["div_yield"] +
        df["score_pe"]        * weights["pe"] +
        df["score_roe"]       * weights["roe"] +
        df["score_pb"]        * weights["pb"] +
        df["score_rsi"]       * weights["rsi"] +
        df["score_ma"]        * weights["ma"]
    ).round(0).astype(int)
    
    # Signal
    df["signal"] = "WATCH"
    df.loc[(df["composite_score"] >= 70) & (df["rsi_14"] < 50), "signal"] = "BUY"
    df.loc[(df["composite_score"] >= 55) & (df["signal"] != "BUY"), "signal"] = "HOLD"
    df.loc[df["composite_score"] < 40, "signal"] = "SELL"
    
    return df


def main():
    fundamentals = pd.read_parquet("data/processed/brvm_fundamentals.parquet")
    
    # Ensure required columns exist with defaults
    for col, default in [("pe_ratio", 12), ("div_yield_pct", 3), ("roe_pct", 15),
                          ("pb_ratio", 2), ("rsi_14", 50), ("price", 5000)]:
        if col not in fundamentals.columns:
            fundamentals[col] = default
        fundamentals[col] = pd.to_numeric(fundamentals[col], errors="coerce").fillna(default)
    
    if "sector" not in fundamentals.columns:
        fundamentals["sector"] = "Unknown"
    fundamentals["sector"] = fundamentals["sector"].fillna("Unknown")
    
    # Try to merge with technical indicators if available
    latest_path = Path("data/processed/brvm_latest.parquet")
    if latest_path.exists():
        latest = pd.read_parquet(latest_path)
        merged = fundamentals.merge(
            latest[["ticker", "rsi_14", "ma_signal", "volatility_20d", "return_pct", "close"]].rename(
                columns={"rsi_14": "rsi_14_tech"}
            ),
            on="ticker", how="left"
        )
        # Prefer technical RSI over fundamental RSI if available
        if "rsi_14_tech" in merged.columns:
            merged["rsi_14"] = merged["rsi_14_tech"].fillna(merged["rsi_14"])
            merged.drop(columns=["rsi_14_tech"], inplace=True)
        merged["ma_signal"] = merged["ma_signal"].fillna("below")
    else:
        merged = fundamentals.copy()
        merged["ma_signal"] = "below"
        merged["volatility_20d"] = 2.0
        merged["return_pct"] = 0.0
    
    # Score
    scored = compute_composite_score(merged)
    scored = scored.sort_values("composite_score", ascending=False).reset_index(drop=True)
    scored["rank"] = range(1, len(scored) + 1)
    
    # Save
    scored.to_parquet("data/processed/brvm_scored.parquet", index=False)
    
    # Sector summary
    agg_dict = {"ticker": "count", "composite_score": "mean"}
    if "pe_ratio" in scored.columns: agg_dict["pe_ratio"] = "mean"
    if "div_yield_pct" in scored.columns: agg_dict["div_yield_pct"] = "mean"
    if "roe_pct" in scored.columns: agg_dict["roe_pct"] = "mean"
    
    sector = scored.groupby("sector").agg(**{
        "nb_stocks": ("ticker", "count"),
        "avg_score": ("composite_score", "mean"),
        **({f"avg_pe": ("pe_ratio", "mean")} if "pe_ratio" in scored.columns else {}),
        **({f"avg_div_yield": ("div_yield_pct", "mean")} if "div_yield_pct" in scored.columns else {}),
        **({f"avg_roe": ("roe_pct", "mean")} if "roe_pct" in scored.columns else {}),
    }).round(1).reset_index().sort_values("avg_score", ascending=False)
    sector.to_parquet("data/processed/brvm_sector_summary.parquet", index=False)
    
    print("[OK] Scoring complete:")
    print(f"   Stocks scored:   {len(scored)}")
    print(f"   BUY signals:     {(scored['signal'] == 'BUY').sum()}")
    print(f"   HOLD signals:    {(scored['signal'] == 'HOLD').sum()}")
    print(f"   WATCH signals:   {(scored['signal'] == 'WATCH').sum()}")
    print(f"   SELL signals:    {(scored['signal'] == 'SELL').sum()}")
    print(f"   Top 3: {', '.join(scored.head(3)['ticker'].tolist())}")


if __name__ == "__main__":
    main()
