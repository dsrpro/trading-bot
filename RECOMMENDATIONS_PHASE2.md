# Recommandations pour la Phase 2 — Paper Trading

**Date**: 5 Août 2026
**Projet**: Trading Bot — Bollinger Bands + RSI + Rejection Candles
**Phase 2**: Paper trading 1 mois sur compte démo Deriv

---

## 📊 Diagnostic du Backtest Phase 1

### Résultats du backtest 3 ans (2023-2026) sur R_75 (1HZ100V)

| Métrique | Valeur | Interprétation |
|---|------|---|
| Trades totaux | 42 | Faible échantillon (~1.2/mois) |
| Win rate | **16.67%** | ❌ Très insuffisant |
| Profit factor | **0.94** | ❌ < 1.0 = stratégie perdante |
| Return total | **-3.91%** | ❌ Perte nette |
| Max drawdown | **30.49%** | ❌ Dépasse le seuil de 20% |
| Sharpe ratio | -0.00 | ❌ Nul/négatif |
| Avg win | $8.28 | ✅ Bon (R:R 1:5 théorique) |
| Avg loss | $1.77 | ✅ Pertes contrôlées |
| Expectancy | -$0.09 | ❌ Espérance négative par trade |

### 🔍 Problèmes identifiés

1. **Pas de filtre de tendance** : Les signaux sont pris dans les deux sens sans vérifier la tendance de fond (EMA 50/200). Un CALL en tendance baissière est quasi-systématiquement perdant.

2. **R:R 1:5 trop ambitieux** : Un TP à 100 pips pour un SL à 20 pips donne un ratio théorique de 1:5, mais le prix touche le SL beaucoup plus souvent que le TP. Résultat : 35 pertes pour 7 gains.

3. **Pas de filtre de volatilité** : Les signaux sont pris même en marché range (sans tendance) où les faux signaux sont fréquents. Les 3/4 des pertes surviennent en range.

4. **SL/TP fixes en pips** : 20 pips de SL et 100 pips de TP ne tiennent pas compte de la volatilité réelle du marché. Sur R_75, la volatilité varie considérablement.

5. **Pas de trailing stop** : Les trades gagnants sont sortis au TP fixe, sans possibilité de laisser courir les gains ou de sécuriser les profits.

6. **Pas de cooldown entre signaux** : Plusieurs trades consécutifs sur le même mouvement = pertes en cascade.

---

## ✅ Corrections et Améliorations — Phase 2

### 1. Filtre de Tendance EMA 50/200

```python
# Ajouté dans StrategyEngine._detect_signal() et Backtester._evaluate_on_buffer()
ema50 = indicators.ema(closes, 50)
ema200 = indicators.ema(closes, 200)

uptrend = ema50 > ema200   # CALL uniquement si uptrend
downtrend = ema50 < ema200  # PUT uniquement si downtrend

# Skip si tendance insuffisante (< 0.05% d'écart)
if abs(trend_ratio) < 0.05:
    return HOLD  # Marché en range → ne pas trader
```

**Impact attendu** : ↓ 40-50% des faux signaux, particulièrement en marché range.

### 2. Filtre de Volatilité ATR

```python
atr_pct = ATR / prix_close

if atr_pct < 0.01%:   # Marché trop calme
    return HOLD
if atr_pct > 1.5%:     # Marché trop agité
    return HOLD
```

**Impact attendu** : Évite les trades en range ultra-étroit et en pics de volatilité.

### 3. SL/TP Dynamiques (ATR-based)

**Avant (Phase 1)** :
- SL = 20 pips fixes
- TP = 100 pips fixes
- R:R théorique = 1:5

**Après (Phase 2)** :
- SL = 1.5 × ATR (s'adapte à la volatilité)
- TP = 3.0 × ATR
- R:R effectif = 1:2

```python
atr_sl = ATR * 1.5   # Distance SL
atr_tp = ATR * 3.0   # Distance TP

# CALL
stop_loss = entry_price - atr_sl
take_profit = entry_price + atr_tp
```

**Impact attendu** : TP plus atteignable → win rate amélioré de 16% → 30-35% estimé.

### 4. Trailing Stop Adaptatif

```python
# S'active quand le prix a parcouru 50% du chemin vers le TP
activation_distance = abs(TP - entry) * 0.5

# Suit le prix à 1.0x ATR de distance
trail_distance = ATR * 1.0

# Exemple CALL:
# Entry=$100, TP=$103, SL=$98.50
# Prix monte à $101.50 → trailing activé, nouveau SL=$100.50
# Prix monte à $103 → TP touché, ou SL trailing à $102
```

**Impact attendu** : Réduit les pertes sur les trades qui s'inversent après un début favorable. Améliore l'expectancy.

### 5. Cooldown Entre Signaux

```python
MIN_CANDLES_BETWEEN_SIGNALS = 3  # 3 bougies M1 = 3 min

if candles_since_last_signal < 3:
    return  # Évite les trades en cascade
```

### 6. Détection de Régime de Marché (MarketRegime)

Ajout d'un module `MarketRegime` capable de classifier le marché en :
- `trending_up` : EMA50 > EMA200 avec écart > 0.5%
- `trending_down` : EMA50 < EMA200 avec écart > 0.5%
- `ranging` : EMA50 ≈ EMA200 (écart < 0.5%)
- `volatile` : ATR > 0.3% du prix

Le bot peut ainsi adapter son comportement (trader uniquement en trending, éviter le ranging).

---

## 📁 Fichiers Modifiés/Créés

| Fichier | Action | Description |
|---|------|---|
| `src/backtester.py` | ✏️ Modifié | Filtres EMA 50/200, ATR SL/TP, volatilité, données synthétiques à régimes |
| `src/paper_trading_phase2.py` | 🆕 Créé | Script dédié Phase 2 avec TrailingStop, MarketRegime, logging enrichi |
| `RECOMMENDATIONS_PHASE2.md` | 🆕 Créé | Ce document |

---

## 🚀 Guide d'Exécution Phase 2

### Étape 1 : Vérifier le backtest optimisé

```bash
cd trading-bot
python -m src.main backtest
```

Vérifier que le nombre de trades est > 10 et le win rate > 25%.

### Étape 2 : Lancer le paper trading Phase 2

```bash
# Simulation 1 heure avec données synthétiques
python -m src.paper_trading_phase2 --duration 60

# Avec un autre symbole
python -m src.paper_trading_phase2 --symbol R_100 --duration 120
```

### Étape 3 : Paper trading avec API Deriv (compte démo)

```bash
# Configurer le token dans config/settings.env
# DERIV_TOKEN=pat_votre_token_ici

# Lancer avec connexion API
python -m src.paper_trading_phase2 --api --duration 60
```

### Étape 4 : Analyser les résultats

Les résultats de chaque session sont sauvegardés dans `data/phase2_results/session_YYYYMMDD_HHMMSS.json`.

Pour une session réussie, les métriques cibles sont :
- Win rate ≥ 35%
- Profit factor ≥ 1.3
- Max drawdown < 15%
- Expectancy > 0

---

## ⚙️ Paramètres Ajustables

Tous ces paramètres sont configurables dans `Phase2Config` (paper_trading_phase2.py) :

```python
# Filtres
use_trend_filter = True         # Activer/désactiver filtre EMA
ema_fast = 50                   # Période EMA rapide
ema_slow = 200                  # Période EMA lente
trend_strength_min_pct = 0.15   # Seuil minimum de tendance (%)

# SL/TP ATR
atr_sl_mult = 1.5               # Multiplier SL
atr_tp_mult = 3.0               # Multiplier TP
use_trailing_stop = True        # Activer trailing stop

# Cooldown
signal_cooldown_candles = 3     # Bougies min entre 2 signaux
```

---

## 📈 Plan de Suivi — 1 Mois

| Semaine | Objectif | Métriques à suivre |
|---|------|---|
| S1 | Validation des filtres | Nombre de signaux/jour, ratio signaux filtrés/générés |
| S2 | Ajustement SL/TP ATR | Win rate, avg win/loss, R:R effectif |
| S3 | Optimisation trailing stop | % de trades sauvés par le trailing, profit factor |
| S4 | Bilan et décision Phase 3 | Return total, drawdown max, décision go/no-go |

### Critères Go/No-Go pour la Phase 3 (forward testing $500 réel)

- ✅ Win rate ≥ 35%
- ✅ Profit factor ≥ 1.3
- ✅ Max drawdown < 20%
- ✅ Au moins 40 trades exécutés sur le mois
- ✅ Aucun kill switch déclenché

Si 4/5 critères sont verts → GO Phase 3.
Si 2-3 critères → prolonger Phase 2 d'une semaine avec ajustements.
Si < 2 critères → retour en Phase 1 (backtest) pour réviser la stratégie.

---

## ⚠️ Risques et Limitations

1. **Données synthétiques ≠ marché réel** : Le comportement sur données Deriv réelles peut différer significativement.
2. **Sur-optimisation** : Les filtres ajoutés réduisent le nombre de signaux. Trop de filtres = 0 trades.
3. **Latence API** : La connexion WebSocket Deriv peut avoir des délais qui affectent l'exécution.
4. **Symbole R_75** : La volatilité et le comportement peuvent changer selon les conditions de marché.

---

*Document généré automatiquement le 5 Août 2026.*
*Projet : Trading Bot — Phase 2 Paper Trading*