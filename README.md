# Trading Bot — Bollinger Bands + RSI + Rejection Candles

Bot de trading algorithmique professionnel pour les indices synthétiques Deriv.
Conforme au **Plan 1** (Plan d'Action) et au **Plan 2** (Cahier des Charges Technique).

## Architecture

```
trading-bot/
├── src/
│   ├── config.py              # Configuration centralisée (.env)
│   ├── logger.py              # Logging structuré JSON + console
│   ├── deriv_client.py        # Client WebSocket asynchrone Deriv
│   ├── data_streamer.py       # Streaming et validation des ticks
│   ├── candle_builder.py      # Construction OHLC (M1, M5...)
│   ├── indicators.py          # Bollinger, RSI, ATR, EMA, MACD (NumPy + TA-Lib)
│   ├── strategy_engine.py     # Moteur de stratégie (scoring + signaux)
│   ├── risk_manager.py        # 2% risk/trade, 5% daily SL, 20% max DD, kill switch
│   ├── order_executor.py      # Exécution dry-run / paper / live
│   ├── contract_monitor.py    # Suivi des contrats ouverts
│   ├── backtester.py          # Backtesting événementiel + métriques
│   └── main.py                # Point d'entrée CLI
├── strategies/
│   └── bollinger_rsi.json     # Configuration de la stratégie
├── tests/
│   └── test_core.py           # 67 tests unitaires (100% passant)
├── config/
│   └── settings.env           # Template de configuration
├── logs/                      # Logs d'activité (auto-généré)
├── requirements.txt
└── README.md
```

## Installation

```bash
cd trading-bot
pip install -r requirements.txt
```

### Optionnel : TA-Lib
```bash
# Windows : télécharger le wheel depuis https://www.lfd.uci.edu/~gohlke/pythonlibs/#ta-lib
pip install TA_Lib‑0.4.28‑cp312‑cp312‑win_amd64.whl

# Linux :
sudo apt-get install ta-lib
pip install TA-Lib
```
Si TA-Lib n'est pas installé, le bot utilise automatiquement NumPy pur (plus lent mais fonctionnel).

## Utilisation

### 1. Backtesting (simulation sur données synthétiques)
```bash
python -m src.main backtest
```

### 2. Dry-Run (simulation temps réel sans API)
```bash
python -m src.main dry-run --duration 10
```
Simule 10 minutes de trading avec des ticks synthétiques, exécute la stratégie complète, et affiche un rapport final.

### 3. Paper Trading (compte démo Deriv)
```bash
# 1. Configurer DERIV_TOKEN dans config/settings.env
#    Token disponible sur https://app.deriv.com/account/api-token
# 2. Lancer
python -m src.main paper
```

### 4. Phase 2 – Paper Trading optimisé
```bash
python -m src.main phase2 --duration 60
python -m src.main phase2 --duration 60 --api
```

### 5. Rapport de risque
```bash
python -m src.main report
```

## Tests

```bash
pytest tests/test_core.py -v
```

**Résultat actuel : 67/67 tests passent (`✓`)**

## Stratégie

### Conditions d'entrée

| Signal | Conditions |
|---|---|
| **CALL** | Close ET Open sous la bande inférieure de Bollinger + RSI < 30 + Rejet baissier (mèche inférieure ≥ 50%) |
| **PUT** | Close ET Open au-dessus de la bande supérieure de Bollinger + RSI > 70 + Rejet haussier (mèche supérieure ≥ 50%) |

### Paramètres

| Paramètre | Valeur |
|---|---|
| Timeframe | M1 / M5 |
| Bollinger | 20, 2σ |
| RSI | 14 |
| SL | 20 pips |
| TP | 100 pips |
| Risk:Reward | 1:5 |

### Risk Management

| Règle | Valeur |
|---|---|
| Risque par trade | 2% du capital |
| Stop-loss quotidien | 5% du capital |
| Drawdown max | 20% du capital |
| Max trades/jour | 2 |
| Taille de lot | Fixe (3-6 mois) |

## Phases de déploiement (Plan 1)

1. **Phase 1** : Backtesting sur 3 ans de données historiques Deriv (2 semaines)
2. **Phase 2** : Paper trading sur compte démo (1 mois)
3. **Phase 3** : Forward testing micro-lots ($500 réel, 2 semaines)
4. **Phase 4** : Mise en production progressive

## Avertissement

Le trading comporte des risques significatifs de perte en capital.
Ce logiciel est un outil technique — il ne constitue pas un conseil financier.
La performance passée ne garantit pas les résultats futurs.

## Licence

Usage interne — Projet privé