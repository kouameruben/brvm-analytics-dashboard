# BRVM Analytics Dashboard -- Marche Boursier UEMOA

> **Probleme :** Les investisseurs de la BRVM (20+ actions, 8 pays) n'ont pas d'outil d'analyse moderne et gratuit. Ce dashboard fournit un scoring automatise, l'analyse technique (RSI, MACD, Bollinger), et un simulateur de portefeuille dividendes.

[![Streamlit](https://img.shields.io/badge/Live_Dashboard-Streamlit-FF4B4B?style=for-the-badge)](https://brvm-analytics.streamlit.app)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-3F4F75?style=flat&logo=plotly&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)

---

## Valeur Pour l'Investisseur

| Fonctionnalite | Ce que ca vous apporte |
|----------------|----------------------|
| Scoring automatise | Classement objectif des 20 actions par attractivite (0-100) |
| Graphiques chandelier | Cours OHLC avec moyennes mobiles MA20/MA50 |
| Indicateurs techniques | RSI, MACD, Bollinger Bands pour chaque action |
| Analyse sectorielle | Comparer banques vs telecom vs industrie en 1 clic |
| Simulateur dividendes | Projection sur 5 ans avec reinvestissement |
| Signaux automatiques | BUY/HOLD/WATCH/SELL bases sur 6 criteres ponderes |

---

## Dashboard Live

**=> [Acceder au dashboard](https://brvm-analytics.streamlit.app)**

4 onglets interactifs :
- **Ranking** -- Classement des 20 actions avec scores et signaux
- **Graphiques** -- Chandelier + RSI + MACD pour chaque action
- **Analyse Sectorielle** -- Comparaison par secteur et par pays
- **Simulateur Dividendes** -- Construire un portefeuille et projeter les gains

---

## Pipeline

```
01_fetch_data.py          02_technical_analysis.py    03_fundamental_scoring.py    dashboard/app.py
+------------------+      +--------------------+      +---------------------+      +--------------+
| 20 tickers BRVM  |----->| RSI, MACD, MA      |----->| Score composite     |----->| Streamlit    |
| 500 jours OHLCV  |      | Bollinger Bands    |      | Signaux BUY/SELL    |      | 4 onglets    |
| Fondamentaux     |      | Volatilite         |      | Ranking             |      | Interactif   |
+------------------+      +--------------------+      +---------------------+      +--------------+
```

---

## Donnees

| Source | Description |
|--------|-------------|
| [stockanalysis.com](https://stockanalysis.com/list/ivory-coast-stock-exchange/) | Liste des 47 actions, prix, market cap, PE, ROE, dividendes, RSI |
| [sikafinance.com](https://www.sikafinance.com/marches/historiques/) | Cours historiques OHLCV (scraping HTML) |
| [brvm.org](https://www.brvm.org) | Reference officielle (donnees fin de journee) |
| [Investing.com](https://fr.investing.com/indices/brvm-10-historical-data) | Indices BRVM 10 / Composite |

> Les donnees sont scrapees automatiquement depuis stockanalysis.com (fondamentaux) et sikafinance.com (cours historiques). Le pipeline tente le scraping en temps reel a chaque execution.

---

## Structure

```
brvm-analytics-dashboard/
+-- python/
|   +-- 01_fetch_data.py           # Ingestion 20 tickers + fondamentaux
|   +-- 02_technical_analysis.py   # RSI, MACD, Bollinger, MA(5,20,50)
|   +-- 03_fundamental_scoring.py  # Score composite + signaux
|   +-- pipeline.py                # Orchestrateur
+-- dashboard/
|   +-- app.py                     # Dashboard Streamlit 4 onglets
+-- data/
|   +-- raw/                       # Donnees sources
|   +-- processed/                 # Parquet transformes
+-- docs/
|   +-- data_dictionary.md         # Dictionnaire des donnees
+-- requirements.txt
+-- .gitignore
+-- LICENSE
+-- README.md
```

---

## Quick Start

```bash
pip install -r requirements.txt
python python/pipeline.py
streamlit run dashboard/app.py
```

---

## Scoring - Methodologie

6 criteres ponderes pour le score composite (0-100) :

| Critere | Poids | Logique |
|---------|-------|---------|
| Rendement Dividende | 25% | Plus eleve = mieux |
| PER | 20% | Plus bas = mieux |
| ROE | 20% | Plus eleve = mieux |
| Price/Book | 10% | Plus bas = mieux |
| RSI neutralite | 15% | Plus proche de 50 = mieux |
| Tendance MA20/50 | 10% | Au-dessus = positif |

Signaux : **BUY** (score >= 70 et RSI < 50) | **HOLD** (>= 55) | **WATCH** (40-54) | **SELL** (< 40)

---

## Resultats (derniere execution)

```
[OK] BRVM data ingestion complete:
   Tickers:      20
   Price history: 10,000 rows
   Sectors:       7

[OK] Technical analysis complete:
   Indicators: MA(5,20,50), RSI(14), MACD, Bollinger(20,2)

[OK] Scoring complete:
   BUY signals:  X stocks
   HOLD signals: X stocks
```

---

## Auteur

**Kouame Ruben** -- Senior Data Analyst | Investisseur BRVM actif depuis 3+ ans
- [LinkedIn](https://www.linkedin.com/in/kouameruben/) | [GitHub](https://github.com/kouameruben)

## Licence

MIT License
