# Dictionnaire des Donnees -- BRVM Analytics Dashboard

## Tables

### brvm_fundamentals (20 lignes)
| Colonne | Type | Exemple | Description |
|---------|------|---------|-------------|
| ticker | VARCHAR(5) PK | SNTS | Code boursier BRVM |
| company | VARCHAR(30) | SONATEL | Nom entreprise |
| sector | VARCHAR(15) | Telecom | Secteur (7 secteurs) |
| country | VARCHAR(20) | Senegal | Pays de l'emetteur |
| price | INTEGER | 17500 | Cours actuel (FCFA) |
| eps | INTEGER | 1420 | Benefice par action |
| dividend | INTEGER | 900 | Dividende par action |
| book_value | INTEGER | 8500 | Valeur comptable |
| pe_ratio | DECIMAL | 12.3 | Price-to-Earnings |
| div_yield_pct | DECIMAL | 5.2 | Rendement dividende (%) |
| roe_pct | DECIMAL | 16.7 | Return on Equity (%) |
| pb_ratio | DECIMAL | 2.06 | Price-to-Book |

### brvm_prices (10,000 lignes)
| Colonne | Type | Description |
|---------|------|-------------|
| date | DATE | Date de cotation |
| ticker | VARCHAR(5) | Code boursier |
| open | INTEGER | Cours ouverture (FCFA) |
| high | INTEGER | Plus haut |
| low | INTEGER | Plus bas |
| close | INTEGER | Cours cloture |
| volume | INTEGER | Volume echange |

### brvm_technical (colonnes ajoutees)
| Colonne | Formule | Description |
|---------|---------|-------------|
| ma_5 / ma_20 / ma_50 | rolling mean | Moyennes mobiles |
| rsi_14 | RSI(14) | Relative Strength Index |
| macd / macd_signal | EMA(12)-EMA(26) | MACD |
| bb_upper / bb_lower | SMA(20) +/- 2*std | Bollinger Bands |
| ma_signal | MA20 vs MA50 | above / below |
| rsi_signal | RSI zones | oversold / neutral / overbought |

### brvm_scored (colonnes ajoutees)
| Colonne | Description |
|---------|-------------|
| composite_score | Score 0-100 (6 criteres ponderes) |
| signal | BUY / HOLD / WATCH / SELL |
| rank | Classement par score |
