"""
02_technical_analysis.py -- Technical Indicators for BRVM Stocks
Author: Kouame Ruben
Description: Computes RSI, MACD, Bollinger Bands, Moving Averages for all tickers
"""

import pandas as pd
import numpy as np
from pathlib import Path


def compute_rsi(series, period=14):
    """Compute Relative Strength Index."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / (loss + 1e-10)
    return 100 - (100 / (1 + rs))


def compute_macd(series, fast=12, slow=26, signal=9):
    """Compute MACD line, signal line, and histogram."""
    ema_fast = series.ewm(span=fast).mean()
    ema_slow = series.ewm(span=slow).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def compute_bollinger(series, period=20, std_dev=2):
    """Compute Bollinger Bands."""
    sma = series.rolling(period).mean()
    std = series.rolling(period).std()
    upper = sma + std_dev * std
    lower = sma - std_dev * std
    return sma, upper, lower


def add_technical_indicators(df):
    """Add all technical indicators to a price DataFrame for one ticker."""
    df = df.sort_values("date").copy()
    close = df["close"]
    
    # Moving Averages
    df["ma_5"]  = close.rolling(5).mean()
    df["ma_20"] = close.rolling(20).mean()
    df["ma_50"] = close.rolling(50).mean()
    
    # RSI
    df["rsi_14"] = compute_rsi(close, 14)
    
    # MACD
    df["macd"], df["macd_signal"], df["macd_hist"] = compute_macd(close)
    
    # Bollinger Bands
    df["bb_middle"], df["bb_upper"], df["bb_lower"] = compute_bollinger(close)
    
    # Signals
    df["ma_signal"] = np.where(df["ma_20"] > df["ma_50"], "above", "below")
    df["rsi_signal"] = np.where(df["rsi_14"] < 30, "oversold",
                       np.where(df["rsi_14"] > 70, "overbought", "neutral"))
    df["bb_signal"] = np.where(close < df["bb_lower"], "oversold",
                      np.where(close > df["bb_upper"], "overbought", "neutral"))
    
    # Daily returns
    df["return_pct"] = close.pct_change() * 100
    df["volatility_20d"] = df["return_pct"].rolling(20).std()
    
    return df


def main():
    prices = pd.read_parquet("data/processed/brvm_prices.parquet")
    
    # Add indicators per ticker
    result = []
    for ticker in prices["ticker"].unique():
        df_t = prices[prices["ticker"] == ticker].copy()
        df_t = add_technical_indicators(df_t)
        result.append(df_t)
    
    enriched = pd.concat(result, ignore_index=True)
    enriched.to_parquet("data/processed/brvm_technical.parquet", index=False)
    
    # Latest snapshot per ticker (for scoring)
    latest = enriched.sort_values("date").groupby("ticker").tail(1).copy()
    latest.to_parquet("data/processed/brvm_latest.parquet", index=False)
    
    print("[OK] Technical analysis complete:")
    print(f"   Enriched data:    {len(enriched):,} rows")
    print(f"   Indicators added: MA(5,20,50), RSI(14), MACD, Bollinger(20,2)")
    print(f"   Latest snapshot:  {len(latest)} tickers")
    
    # Summary
    oversold = latest[latest["rsi_signal"] == "oversold"]
    overbought = latest[latest["rsi_signal"] == "overbought"]
    print(f"   RSI oversold:     {len(oversold)} stocks")
    print(f"   RSI overbought:   {len(overbought)} stocks")


if __name__ == "__main__":
    main()
