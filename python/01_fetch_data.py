"""
01_fetch_data.py -- BRVM Stock Market Data Ingestion (Real Data Only)
Author: Kouame Ruben
Sources:
  - stockanalysis.com  => Stock list, fundamentals, statistics
  - sikafinance.com    => Historical prices (OHLCV)
"""

import pandas as pd
import numpy as np
import time
import re
from pathlib import Path
from datetime import datetime

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Mapping stockanalysis ticker => sikafinance code
SIKA_CODES = {
    "SNTS": "SNTS.sn", "ORAC": "ORAC.ci", "SGBC": "SGBC.ci", "ECOC": "ECOC.ci",
    "SIBC": "SIBC.ci", "SLBC": "SLBC.ci", "UNLC": "UNLC.ci", "ETIT": "ETIT.tg",
    "CBIBF":"CBIBF.bf","BICC": "BICC.ci", "STBC": "STBC.ci", "BOAC": "BOAC.ci",
    "NSBC": "NSBC.ci", "BOAB": "BOAB.bj", "CFAC": "CFAC.ci", "BICB": "BICB.bj",
    "ORGT": "ORGT.tg", "BOAS": "BOAS.sn", "BOABF":"BOABF.bf","SPHC": "SPHC.ci",
    "ONTBF":"ONTBF.bf","SOGC": "SOGC.ci", "CIEC": "CIEC.ci", "TTLC": "TTLC.ci",
    "PALC": "PALC.ci", "SHEC": "SHEC.ci", "BOAM": "BOAM.ml", "SDSC": "SDSC.ci",
    "SDCC": "SDCC.ci", "PRSC": "PRSC.ci", "SEMC": "SEMC.ci", "FTSC": "FTSC.ci",
    "UNXC": "UNXC.ci", "SVOC": "SVOC.ci", "TTLS": "TTLS.sn", "SMBC": "SMBC.ci",
    "BNBC": "BNBC.ci", "NEIC": "NEIC.ci", "SAFC": "SAFC.ci", "STAC": "STAC.ci",
    "NTLC": "NTLC.ci", "TTRC": "TTRC.ci", "ABJC": "ABJC.ci", "SIVC": "SIVC.ci",
    "BOABF":"BOABF.bf","CABC": "CABC.ci", "MVOM": "MVOM.ci", "LNBB": "LNBB.bj",
}

# Reference: French names + BRVM sectors for all tickers
BRVM_REF = {
    "SNTS":  {"nom": "SONATEL",                 "secteur": "Telecom"},
    "ORAC":  {"nom": "ORANGE CI",               "secteur": "Telecom"},
    "SGBC":  {"nom": "SGBCI",                   "secteur": "Banque"},
    "ECOC":  {"nom": "ECOBANK CI",              "secteur": "Banque"},
    "SIBC":  {"nom": "SIB",                     "secteur": "Banque"},
    "SLBC":  {"nom": "SOLIBRA",                 "secteur": "Industrie"},
    "UNLC":  {"nom": "UNILEVER CI",             "secteur": "Industrie"},
    "ETIT":  {"nom": "ECOBANK TG",              "secteur": "Banque"},
    "CBIBF": {"nom": "CORIS BANK INTL",         "secteur": "Banque"},
    "BICC":  {"nom": "BICICI",                  "secteur": "Banque"},
    "STBC":  {"nom": "SITAB",                   "secteur": "Industrie"},
    "BOAC":  {"nom": "BOA CI",                  "secteur": "Banque"},
    "NSBC":  {"nom": "NSIA BANQUE",             "secteur": "Banque"},
    "BOAB":  {"nom": "BOA BENIN",               "secteur": "Banque"},
    "CFAC":  {"nom": "CFAO MOTORS CI",          "secteur": "Distribution"},
    "BICB":  {"nom": "BIIC BENIN",              "secteur": "Banque"},
    "ORGT":  {"nom": "ORAGROUP TG",             "secteur": "Banque"},
    "BOAS":  {"nom": "BOA SENEGAL",             "secteur": "Banque"},
    "BOABF": {"nom": "BOA BURKINA FASO",        "secteur": "Banque"},
    "SPHC":  {"nom": "SAPH CI",                 "secteur": "Agriculture"},
    "ONTBF": {"nom": "ONATEL BF",               "secteur": "Telecom"},
    "SOGC":  {"nom": "SOGB CI",                 "secteur": "Agriculture"},
    "CIEC":  {"nom": "CIE CI",                  "secteur": "Services publics"},
    "TTLC":  {"nom": "TOTALENERGIES CI",        "secteur": "Energie"},
    "PALC":  {"nom": "PALM CI",                 "secteur": "Agriculture"},
    "SHEC":  {"nom": "VIVO ENERGY CI",          "secteur": "Energie"},
    "BOAM":  {"nom": "BOA MALI",                "secteur": "Banque"},
    "SDSC":  {"nom": "AGL (ex-BOLLORE)",        "secteur": "Transport"},
    "SDCC":  {"nom": "SODECI",                  "secteur": "Services publics"},
    "PRSC":  {"nom": "TRACTAFRIC MOTORS CI",    "secteur": "Distribution"},
    "SEMC":  {"nom": "CROWN SIEM",              "secteur": "Assurance"},
    "FTSC":  {"nom": "FILTISAC CI",             "secteur": "Industrie"},
    "UNXC":  {"nom": "UNIWAX CI",               "secteur": "Industrie"},
    "SVOC":  {"nom": "SUCRIVOIRE",              "secteur": "Agriculture"},
    "TTLS":  {"nom": "TOTAL SENEGAL",           "secteur": "Energie"},
    "SMBC":  {"nom": "SMB CI",                  "secteur": "Banque"},
    "BNBC":  {"nom": "BERNABE CI",              "secteur": "Distribution"},
    "NEIC":  {"nom": "NEI-CEDA CI",             "secteur": "Industrie"},
    "SAFC":  {"nom": "SAFCA CI",                "secteur": "Finance"},
    "STAC":  {"nom": "SETAO CI",                "secteur": "Industrie"},
    "NTLC":  {"nom": "NESTLE CI",               "secteur": "Industrie"},
    "TTRC":  {"nom": "TRACTAFRIC CI",           "secteur": "Distribution"},
    "ABJC":  {"nom": "SERVAIR ABIDJAN",         "secteur": "Distribution"},
    "SIVC":  {"nom": "AIR LIQUIDE CI",          "secteur": "Industrie"},
    "CABC":  {"nom": "SICABLE CI",              "secteur": "Industrie"},
    "MVOM":  {"nom": "MOVIS CI",                "secteur": "Transport"},
    "LNBB":  {"nom": "LOTERIE DU BENIN",        "secteur": "Autres"},
    "BOABF": {"nom": "BOA BURKINA FASO",        "secteur": "Banque"},
}


def parse_num(text):
    """Parse number from text: '2.90T' => 2900000000000, '28,995.0' => 28995"""
    if not text or text.strip() in ("-", "n/a", "N/A", ""):
        return None
    text = text.strip().replace(",", "").replace("%", "").replace("\xa0", "")
    multiplier = 1
    if text.endswith("T"):
        multiplier = 1e12; text = text[:-1]
    elif text.endswith("B"):
        multiplier = 1e9; text = text[:-1]
    elif text.endswith("M"):
        multiplier = 1e6; text = text[:-1]
    elif text.endswith("K"):
        multiplier = 1e3; text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def parse_fr_num(text):
    """Parse French-formatted number: '29 000' => 29000"""
    if not text or text.strip() in ("-", ""):
        return None
    text = text.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    text = text.replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


# ============================================================
# SOURCE 1: stockanalysis.com — Stock list + fundamentals
# ============================================================

def scrape_stock_list():
    """Scrape the BRVM stock list from stockanalysis.com."""
    url = "https://stockanalysis.com/list/ivory-coast-stock-exchange/"
    print("   [1/3] Scraping stock list from stockanalysis.com...")
    
    resp = requests.get(url, headers=HEADERS, verify=False, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    
    table = soup.find("table")
    if not table:
        raise ValueError("Could not find stock table on stockanalysis.com")
    
    rows = table.find_all("tr")
    records = []
    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) >= 7:
            # Find ticker from link
            link = cols[1].find("a")
            ticker = link.text.strip() if link else cols[1].get_text(strip=True)
            company = cols[2].get_text(strip=True)
            
            records.append({
                "ticker": ticker,
                "company": company,
                "market_cap": parse_num(cols[3].get_text(strip=True)),
                "price": parse_num(cols[4].get_text(strip=True)),
                "change_pct": parse_num(cols[5].get_text(strip=True)),
                "revenue": parse_num(cols[6].get_text(strip=True)),
            })
    
    df = pd.DataFrame(records)
    df = df[df["price"].notna()].reset_index(drop=True)
    
    # Enrich with French names and BRVM sectors
    df["company"] = df["ticker"].map(lambda t: BRVM_REF.get(t, {}).get("nom", df.loc[df["ticker"]==t, "company"].values[0] if len(df.loc[df["ticker"]==t]) > 0 else t))
    df["sector"] = df["ticker"].map(lambda t: BRVM_REF.get(t, {}).get("secteur", "Autres"))
    
    print(f"         Found {len(df)} stocks")
    return df


def scrape_statistics(ticker):
    """Scrape fundamental statistics for one ticker from stockanalysis.com."""
    url = f"https://stockanalysis.com/quote/brvm/{ticker}/statistics/"
    try:
        resp = requests.get(url, headers=HEADERS, verify=False, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        stats = {}
        # Find all key-value pairs in tables
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) == 2:
                    key = cells[0].get_text(strip=True).lower()
                    val = cells[1].get_text(strip=True)
                    
                    if "pe ratio" == key:
                        stats["pe_ratio"] = parse_num(val)
                    elif "forward pe" == key:
                        stats["forward_pe"] = parse_num(val)
                    elif "pb ratio" == key:
                        stats["pb_ratio"] = parse_num(val)
                    elif "return on equity" in key:
                        stats["roe_pct"] = parse_num(val)
                    elif "earnings per share" in key:
                        stats["eps"] = parse_num(val)
                    elif "beta" in key and "beta" == key.split("(")[0].strip():
                        stats["beta"] = parse_num(val)
                    elif "relative strength" in key:
                        stats["rsi_14"] = parse_num(val)
                    elif "dividend" in key and "yield" not in key and "date" not in key:
                        pass
                    elif "book value per share" == key:
                        stats["book_value"] = parse_num(val)
        
        # Also extract dividend info from overview page header
        text = soup.get_text()
        div_match = re.search(r'Dividend.*?([\d,.]+)\s*\(([\d,.]+)%\)', text)
        if div_match:
            stats["dividend"] = parse_num(div_match.group(1))
            stats["div_yield_pct"] = parse_num(div_match.group(2))
        
        return stats
    except Exception:
        return {}


def scrape_all_statistics(tickers):
    """Scrape statistics for all tickers."""
    print(f"   [2/3] Scraping statistics for {len(tickers)} stocks...")
    all_stats = {}
    for i, ticker in enumerate(tickers):
        print(f"         {i+1}/{len(tickers)} {ticker}...", end=" ", flush=True)
        stats = scrape_statistics(ticker)
        all_stats[ticker] = stats
        n_found = len([v for v in stats.values() if v is not None])
        print(f"{n_found} metrics")
        time.sleep(0.5)
    return all_stats


# ============================================================
# SOURCE 2: sikafinance.com — Historical prices
# ============================================================

def scrape_sika_history(sika_code):
    """Scrape historical prices from Sikafinance."""
    url = f"https://www.sikafinance.com/marches/historiques/{sika_code}"
    try:
        resp = requests.get(url, headers=HEADERS, verify=False, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            return None
        
        records = []
        for row in table.find_all("tr")[1:]:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) >= 6:
                try:
                    date = datetime.strptime(cols[0].strip(), "%d/%m/%Y").date()
                    records.append({
                        "date": date,
                        "close":  parse_fr_num(cols[1]),
                        "low":    parse_fr_num(cols[2]),
                        "high":   parse_fr_num(cols[3]),
                        "open":   parse_fr_num(cols[4]),
                        "volume": parse_fr_num(cols[5]),
                    })
                except (ValueError, IndexError):
                    continue
        
        if records:
            df = pd.DataFrame(records).dropna(subset=["close"])
            return df.sort_values("date").reset_index(drop=True)
        return None
    except Exception:
        return None


def scrape_all_history(tickers):
    """Scrape historical prices for all tickers from Sikafinance."""
    print(f"   [3/3] Scraping price history from sikafinance.com...")
    all_prices = []
    success = 0
    
    for i, ticker in enumerate(tickers):
        sika_code = SIKA_CODES.get(ticker)
        if not sika_code:
            print(f"         {i+1}/{len(tickers)} {ticker}... no sika code, skipping")
            continue
        
        print(f"         {i+1}/{len(tickers)} {ticker}...", end=" ", flush=True)
        df = scrape_sika_history(sika_code)
        if df is not None and len(df) > 0:
            df["ticker"] = ticker
            all_prices.append(df)
            print(f"{len(df)} rows")
            success += 1
        else:
            print("no data")
        time.sleep(0.5)
    
    print(f"         Scraped {success}/{len(tickers)} tickers successfully")
    if all_prices:
        return pd.concat(all_prices, ignore_index=True)
    return None


# ============================================================
# MAIN
# ============================================================

def main():
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    
    # Step 1: Get stock list from stockanalysis.com
    stock_list = scrape_stock_list()
    tickers = stock_list["ticker"].tolist()
    
    # Step 2: Get statistics for each ticker
    all_stats = scrape_all_statistics(tickers[:25])  # Top 25 by market cap
    
    # Build fundamentals table
    fundamentals = stock_list.head(25).copy()
    for ticker in fundamentals["ticker"]:
        stats = all_stats.get(ticker, {})
        for key, val in stats.items():
            fundamentals.loc[fundamentals["ticker"] == ticker, key] = val
    
    # Fill missing columns
    for col in ["pe_ratio", "pb_ratio", "roe_pct", "eps", "div_yield_pct", "dividend",
                "book_value", "beta", "rsi_14", "forward_pe"]:
        if col not in fundamentals.columns:
            fundamentals[col] = None
    
    fundamentals.to_csv("data/raw/brvm_fundamentals.csv", index=False)
    fundamentals.to_parquet("data/processed/brvm_fundamentals.parquet", index=False)
    
    # Step 3: Get historical prices from Sikafinance
    prices = scrape_all_history(tickers[:25])
    
    if prices is not None and len(prices) > 0:
        prices.to_csv("data/raw/brvm_prices.csv", index=False)
        prices.to_parquet("data/processed/brvm_prices.parquet", index=False)
        price_source = f"sikafinance.com ({prices['ticker'].nunique()} tickers)"
    else:
        print("   [!] No historical prices scraped. Dashboard will use fundamentals only.")
        # Create minimal prices from stock_list current price
        min_prices = stock_list.head(25).copy()
        min_prices["date"] = datetime.now().date()
        min_prices["open"] = min_prices["price"]
        min_prices["high"] = min_prices["price"]
        min_prices["low"] = min_prices["price"]
        min_prices["close"] = min_prices["price"]
        min_prices["volume"] = 0
        min_prices = min_prices[["date", "ticker", "open", "high", "low", "close", "volume"]]
        min_prices.to_csv("data/raw/brvm_prices.csv", index=False)
        min_prices.to_parquet("data/processed/brvm_prices.parquet", index=False)
        prices = min_prices
        price_source = "stockanalysis.com (current day only)"
    
    # Save metadata
    with open("data/processed/data_source.txt", "w", encoding="utf-8") as f:
        f.write(f"Fundamentals: stockanalysis.com\n")
        f.write(f"Prices: {price_source}\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"Tickers: {len(fundamentals)}\n")
    
    print(f"[OK] BRVM data ingestion complete:")
    print(f"   Fundamentals:  stockanalysis.com ({len(fundamentals)} stocks)")
    print(f"   Prices:        {price_source}")
    print(f"   Total rows:    {len(prices):,}")
    print(f"   Sectors:       {stock_list['ticker'].nunique()} tickers from BRVM")


if __name__ == "__main__":
    main()
