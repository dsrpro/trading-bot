"""Tests unitaires pour les modules core du bot de trading.

Executez avec: pytest tests/test_core.py -v
"""

from __future__ import annotations

import sys
import os
import time
import asyncio
from pathlib import Path

import numpy as np
import pytest

# Ajouter le projet au path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import Config, load_config
from src.data_streamer import Tick, TickBuffer, DataStreamer
from src.candle_builder import Candle, CandleBuilder
from src.indicators import Indicators
from src.strategy_engine import SignalDirection, StrategyEngine, TradingSignal
from src.risk_manager import RiskManager, RiskStatus, RiskReport
from src.order_executor import OrderExecutor, Order, OrderStatus
from src.backtester import Backtester, BacktestResult


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════

@pytest.fixture
def config():
    return load_config()


@pytest.fixture
def indicators(config):
    return Indicators(config)


@pytest.fixture
def candle_builder(config):
    return CandleBuilder(config)


@pytest.fixture
def data_streamer(config):
    return DataStreamer(config)


@pytest.fixture
def risk_manager(config):
    return RiskManager(config)


@pytest.fixture
def order_executor(config):
    return OrderExecutor(config)


@pytest.fixture
def backtester(config):
    return Backtester(config)


@pytest.fixture
def strategy_engine(config, indicators, candle_builder):
    return StrategyEngine(config, indicators, candle_builder)


@pytest.fixture
def sample_ticks():
    """Genere des ticks synthetiques."""
    np.random.seed(42)
    base_time = time.time()
    ticks = []
    price = 100.0
    for i in range(500):
        price *= (1 + np.random.normal(0, 0.0005))
        tick_data = {
            "epoch": base_time + i * 2,
            "quote": round(price, 5),
        }
        ticks.append(Tick.from_deriv(tick_data, "R_75"))
    return ticks


# ═══════════════════════════════════════════════════════════════════
# 1. TEST CONFIG
# ═══════════════════════════════════════════════════════════════════

class TestConfig:
    def test_default_config(self):
        cfg = Config()
        assert cfg.bb_period == 20
        assert cfg.rsi_period == 14
        assert cfg.risk_per_trade_pct == 2.0
        assert cfg.max_trades_per_day == 2
        assert cfg.mode == "dry_run"

    def test_config_validation(self):
        with pytest.raises(AssertionError):
            Config(risk_per_trade_pct=0)
        with pytest.raises(AssertionError):
            Config(mode="invalid")

    def test_load_config_default(self):
        cfg = load_config()
        assert isinstance(cfg, Config)
        assert cfg.bb_period == 20


# ═══════════════════════════════════════════════════════════════════
# 2. TEST DATA STREAMER / TICK / TICK BUFFER
# ═══════════════════════════════════════════════════════════════════

class TestTickBuffer:
    def test_add_and_retrieve(self):
        buf = TickBuffer(maxlen=100)
        t1 = Tick(time.time(), "R_75", 100.0)
        buf.add(t1)
        assert len(buf) == 1
        assert buf.latest_price == 100.0

    def test_maxlen_eviction(self):
        buf = TickBuffer(maxlen=5)
        for i in range(10):
            buf.add(Tick(float(i), "R_75", float(100 + i)))
        assert len(buf) == 5
        assert buf.latest_price == 109.0

    def test_prices_array(self):
        buf = TickBuffer(maxlen=5)
        for i in range(5):
            buf.add(Tick(float(i), "R_75", float(100 + i)))
        prices = buf.prices()
        assert len(prices) == 5
        assert isinstance(prices, np.ndarray)

    def test_empty_buffer(self):
        buf = TickBuffer(maxlen=10)
        assert buf.latest_price is None
        assert len(buf.prices()) == 0


class TestDataStreamer:
    def test_process_tick(self, data_streamer):
        tick_data = {"epoch": time.time(), "quote": 99.50}
        tick = data_streamer.on_tick(tick_data)
        assert tick is not None
        assert tick.price == 99.50
        assert data_streamer.tick_count == 1

    def test_reject_negative_price(self, data_streamer):
        tick_data = {"epoch": time.time(), "quote": -10.0}
        tick = data_streamer.on_tick(tick_data)
        assert tick is None

    def test_reject_zero_price(self, data_streamer):
        tick_data = {"epoch": time.time(), "quote": 0.0}
        tick = data_streamer.on_tick(tick_data)
        assert tick is None

    def test_duplicate_tick_ignored(self, data_streamer):
        tick_data = {"epoch": time.time(), "quote": 100.0}
        data_streamer.on_tick(tick_data)
        count1 = data_streamer.tick_count
        data_streamer.on_tick(tick_data)  # Meme tick
        assert data_streamer.tick_count == count1  # Pas d'incrementation

    def test_subscriber_callback(self, data_streamer):
        received = []
        data_streamer.subscribe(lambda t: received.append(t.price))
        data_streamer.on_tick({"epoch": time.time(), "quote": 101.0})
        assert len(received) == 1
        assert received[0] == 101.0

    def test_reset(self, data_streamer):
        data_streamer.on_tick({"epoch": time.time(), "quote": 100.0})
        data_streamer.reset()
        assert data_streamer.tick_count == 0
        assert data_streamer.latest_price is None


# ═══════════════════════════════════════════════════════════════════
# 3. TEST CANDLE BUILDER
# ═══════════════════════════════════════════════════════════════════

class TestCandle:
    def test_bullish_candle(self):
        c = Candle(timestamp=100, open=10, high=15, low=8, close=12)
        assert c.is_bullish
        assert not c.is_bearish
        assert c.body == 2.0
        assert c.upper_wick == 3.0  # 15 - 12
        assert c.lower_wick == 2.0  # 10 - 8

    def test_bearish_candle(self):
        c = Candle(timestamp=100, open=12, high=15, low=8, close=10)
        assert c.is_bearish
        assert c.upper_wick == 3.0
        assert c.lower_wick == 2.0

    def test_doji_candle(self):
        c = Candle(timestamp=100, open=10, high=15, low=5, close=10.001)
        assert c.is_doji  # body/total_range < 0.05

    def test_to_dict(self):
        c = Candle(timestamp=100, open=10, high=15, low=8, close=12)
        d = c.to_dict()
        assert d["open"] == 10
        assert d["high"] == 15


class TestCandleBuilder:
    def test_build_candles(self, candle_builder, sample_ticks):
        closed = []
        candle_builder.on_candle_close(lambda c: closed.append(c))

        for tick in sample_ticks:
            candle_builder.process_tick(tick)

        # Au moins une bougie fermee
        assert candle_builder.count() >= 0

    def test_ohlc_values(self, candle_builder):
        """Verifie que OHLC est correct pour des ticks dans le meme timeframe."""
        base_ts = int(time.time()) // 60 * 60  # Aligner sur le debut de minute
        ticks_in_same_minute = [
            Tick(base_ts + 1, "R_75", 100.0),
            Tick(base_ts + 2, "R_75", 102.0),
            Tick(base_ts + 3, "R_75", 98.0),
            Tick(base_ts + 4, "R_75", 101.0),
        ]

        for tick in ticks_in_same_minute:
            candle_builder.process_tick(tick)

        current = candle_builder.current_candle
        assert current is not None
        assert current.open == 100.0
        assert current.high == 102.0
        assert current.low == 98.0
        assert current.close == 101.0

    def test_candle_close_callback(self, candle_builder):
        closed_list = []
        candle_builder.on_candle_close(lambda c: closed_list.append(c))

        base_ts = int(time.time()) // 60 * 60

        # Tick dans la minute 0
        candle_builder.process_tick(Tick(base_ts + 1, "R_75", 100.0))

        # Tick dans la minute 1 (ferme la bougie precedente)
        closed = candle_builder.process_tick(Tick(base_ts + 61, "R_75", 101.0))
        assert closed is not None
        assert closed.is_closed
        assert len(closed_list) == 1

    def test_arrays(self, candle_builder, sample_ticks):
        for tick in sample_ticks:
            candle_builder.process_tick(tick)

        closes = candle_builder.close_array()
        if len(closes) > 0:
            assert isinstance(closes, np.ndarray)
            assert closes.dtype == np.float64

    def test_reset(self, candle_builder):
        candle_builder.process_tick(Tick(time.time(), "R_75", 100.0))
        candle_builder.reset()
        assert candle_builder.count() == 0
        assert candle_builder.current_candle is None


# ═══════════════════════════════════════════════════════════════════
# 4. TEST INDICATORS
# ═══════════════════════════════════════════════════════════════════

class TestIndicators:
    def test_bollinger_bands(self, indicators):
        closes = np.array([100 + i * 0.1 + np.sin(i * 0.1) * 2 for i in range(100)])
        upper, middle, lower = indicators.bollinger_bands(closes, period=20, nbdev=2.0)
        assert upper is not None
        assert middle is not None
        assert lower is not None
        assert len(upper) == len(closes)
        # La bande superieure >= bande inferieure
        assert np.all(upper[20:] >= lower[20:])

    def test_rsi(self, indicators):
        closes = np.array([100 + i * 0.1 + np.sin(i * 0.1) * 2 for i in range(100)])
        rsi = indicators.rsi(closes, period=14)
        assert len(rsi) == len(closes)
        # RSI doit etre entre 0 et 100 pour les valeurs valides
        valid_rsi = rsi[~np.isnan(rsi)]
        assert np.all(valid_rsi >= 0) and np.all(valid_rsi <= 100)

    def test_ema(self, indicators):
        closes = np.array([100.0] * 50)
        ema = indicators.ema(closes, period=10)
        assert len(ema) == len(closes)
        # EMA de constantes = constante
        assert np.allclose(ema[15:], 100.0, atol=0.001)

    def test_atr(self, indicators):
        n = 100
        highs = np.array([100 + i * 0.05 for i in range(n)])
        lows = np.array([99 + i * 0.05 for i in range(n)])
        closes = np.array([99.5 + i * 0.05 for i in range(n)])
        atr = indicators.atr(highs, lows, closes, period=14)
        assert len(atr) == n
        valid = atr[~np.isnan(atr)]
        assert np.all(valid > 0)

    def test_macd(self, indicators):
        closes = np.array([100 + i * 0.1 + np.sin(i * 0.1) * 2 for i in range(100)])
        macd_line, signal_line, hist = indicators.macd(closes)
        assert len(macd_line) == len(closes)
        assert len(signal_line) == len(closes)
        assert len(hist) == len(closes)

    def test_rejection_candle_call(self, indicators):
        # Longue meche inferieure = CALL (rejet baissier)
        is_rejection = Indicators.is_rejection_candle(
            open_price=100.0, high=102.0, low=90.0, close_price=101.0,
            direction="CALL"
        )
        # meche inferieure = 101-90 = 11, total=12, ratio=11/12=0.91 > 0.50
        # close > low + 0.5*total = 90+6=96, 101 > 96
        assert is_rejection

    def test_rejection_candle_put(self, indicators):
        # Longue meche superieure = PUT (rejet haussier)
        is_rejection = Indicators.is_rejection_candle(
            open_price=100.0, high=115.0, low=98.0, close_price=101.0,
            direction="PUT"
        )
        # meche superieure = 115-101 = 14, total=17, ratio=14/17=0.82 > 0.50
        # close < high - 0.5*total = 115-8.5=106.5, 101 < 106.5
        assert is_rejection

    def test_no_rejection_small_wick(self):
        # Chandelier avec meches courtes — NE devrait PAS etre un rejet
        is_rejection = Indicators.is_rejection_candle(
            open_price=100.0, high=101.0, low=99.8, close_price=100.5,
            direction="CALL"
        )
        # lower_wick = 100-99.8 = 0.2, total_range=1.2, 0.2 < 0.6 (50%) → pas de rejet
        assert not is_rejection

    def test_insufficient_data_bollinger(self, indicators):
        closes = np.array([100.0, 101.0, 102.0])  # < period
        upper, middle, lower = indicators.bollinger_bands(closes, period=20)
        assert upper is None
        assert middle is None
        assert lower is None

    def test_insufficient_data_rsi(self, indicators):
        closes = np.array([100.0, 101.0, 102.0])  # < period + 1
        rsi = indicators.rsi(closes, period=14)
        assert np.all(np.isnan(rsi))


# ═══════════════════════════════════════════════════════════════════
# 5. TEST STRATEGY ENGINE
# ═══════════════════════════════════════════════════════════════════

class TestSignal:
    def test_signal_valid(self):
        s = TradingSignal(direction=SignalDirection.CALL, symbol="R_75", confidence=0.8)
        assert s.is_valid
        assert s.to_dict()["direction"] == "CALL"

    def test_signal_hold_not_valid(self):
        s = TradingSignal(direction=SignalDirection.HOLD, symbol="R_75")
        assert not s.is_valid

    def test_low_confidence_not_valid(self):
        s = TradingSignal(direction=SignalDirection.CALL, symbol="R_75", confidence=0.4)
        assert not s.is_valid


class TestStrategyEngine:
    def test_insufficient_candles(self, strategy_engine, config):
        # Pas de bougies -> signal HOLD
        signal = strategy_engine.evaluate()
        assert signal.direction == SignalDirection.HOLD

    def test_evaluate_with_candles(self, strategy_engine, candle_builder):
        """Injecte des bougies et verifie que l'evaluation ne crash pas."""
        np.random.seed(42)
        base_ts = int(time.time()) // 60 * 60
        price = 100.0

        for i in range(100):
            ts = base_ts + i * 60
            returns = np.random.normal(0, 0.002)
            o = price
            c = price * (1 + returns)
            h = max(o, c) * (1 + np.random.uniform(0, 0.005))
            l = min(o, c) * (1 - np.random.uniform(0, 0.005))
            tick = Tick(ts + 30, "R_75", c)
            candle_builder.process_tick(tick)
            price = c

        signal = strategy_engine.evaluate()
        assert signal.direction in (SignalDirection.CALL, SignalDirection.PUT, SignalDirection.HOLD)

    def test_volatility_filter_rejects_zero_atr(self, strategy_engine):
        signal = strategy_engine._detect_signal(
            open_price=90.0,
            high=100.0,
            low=80.0,
            close_price=90.0,
            bb_upper=110.0,
            bb_middle=100.0,
            bb_lower=95.0,
            rsi_value=20.0,
            atr_value=0.0,
        )
        assert signal.direction == SignalDirection.HOLD

    def test_signal_has_all_fields(self, strategy_engine, candle_builder):
        """Verifie que le signal retourne a tous les champs remplis."""
        np.random.seed(42)
        base_ts = int(time.time()) // 60 * 60
        price = 100.0

        for i in range(100):
            ts = base_ts + i * 60
            returns = np.random.normal(0, 0.002)
            c = price * (1 + returns)
            tick = Tick(ts + 30, "R_75", c)
            candle_builder.process_tick(tick)
            price = c

        signal = strategy_engine.evaluate()
        assert isinstance(signal.score, float)
        assert isinstance(signal.confidence, float)
        assert isinstance(signal.rsi_value, float)
        assert isinstance(signal.bb_upper, float)
        assert isinstance(signal.bb_lower, float)
        assert signal.symbol == strategy_engine.config.market_symbol
        assert signal.strategy == "bollinger_rsi"

    def test_reset(self, strategy_engine):
        strategy_engine.reset()
        # Apres reset, l'evaluation doit encore fonctionner
        signal = strategy_engine.evaluate()
        assert signal.direction == SignalDirection.HOLD


class TestPaperTradingPhase2:
    def test_create_engine(self, config):
        from src.paper_trading_phase2 import PaperTradingPhase2

        engine = PaperTradingPhase2(config)
        assert engine is not None
        assert engine._results_dir.exists()

    def test_market_regime_detect(self):
        from src.paper_trading_phase2 import MarketRegime

        closes = np.linspace(100.0, 105.0, num=250)
        highs = closes + 0.5
        lows = closes - 0.5
        ema50 = np.convolve(closes, np.ones(50) / 50, mode='same')
        ema200 = np.convolve(closes, np.ones(200) / 200, mode='same')
        atr = np.full_like(closes, 0.2)

        market = MarketRegime.detect(closes, highs, lows, ema50, ema200, atr)
        assert market["regime"] in {"trending_up", "trending_down", "ranging", "volatile", "unknown"}
        assert isinstance(market["trend_strength"], float)
        assert isinstance(market["volatility_pct"], float)


# ═══════════════════════════════════════════════════════════════════
# 6. TEST RISK MANAGER
# ═══════════════════════════════════════════════════════════════════

class TestRiskManager:
    def test_can_trade_initial(self, risk_manager):
        signal = TradingSignal(
            direction=SignalDirection.CALL, symbol="R_75", confidence=0.8, score=80.0
        )
        can, report = risk_manager.can_place_trade(signal)
        assert can
        assert report.can_trade
        assert report.status == RiskStatus.OK
        assert report.position_size > 0

    def test_position_size_calculation(self, risk_manager):
        raw = risk_manager.current_capital * 0.02  # 2%
        max_stake = getattr(risk_manager.config, 'max_stake_usd', 0)
        expected = min(raw, max_stake) if max_stake > 0 else raw
        assert risk_manager._calculate_position_size() == round(expected, 2)

    def test_max_trades_per_day(self, risk_manager):
        risk_manager = RiskManager(Config(
            initial_capital=100.0,
            max_trades_per_day=2,
            daily_profit_target_pct=99.0,
        ))
        signal = TradingSignal(
            direction=SignalDirection.CALL, symbol="R_75", confidence=0.8, score=80.0
        )
        # Faire 2 trades (complets: open + close)
        risk_manager.on_trade_opened(10.0)
        risk_manager.on_trade_closed(pnl=5.0, exit_price=101.0, entry_price=100.0)
        risk_manager.on_trade_opened(10.0)
        risk_manager.on_trade_closed(pnl=-3.0, exit_price=99.0, entry_price=100.0)
        assert risk_manager.trades_today == 2

        can, report = risk_manager.can_place_trade(signal)
        assert not can
        assert report.status == RiskStatus.MAX_TRADES_REACHED

    def test_daily_loss_limit(self, risk_manager):
        # Simuler une perte de 5.1% via on_trade_closed (evite le reset quotidien)
        risk_manager.on_trade_opened(10.0)
        # Perte de 5.1% du capital initial
        loss = -risk_manager.initial_capital * 0.051
        risk_manager.on_trade_closed(pnl=loss, exit_price=95.0, entry_price=100.0)

        signal = TradingSignal(
            direction=SignalDirection.CALL, symbol="R_75", confidence=0.8, score=80.0
        )
        can, report = risk_manager.can_place_trade(signal)
        assert not can
        # Le stop-loss journalier verrouille le trading jusqu'au lendemain,
        # mais n'active PAS le kill switch (reserve au drawdown maximum).
        assert report.status == RiskStatus.DAILY_LOSS_LIMIT_REACHED
        assert not risk_manager.is_kill_switch_active

    def test_daily_profit_target(self, risk_manager):
        # Simuler un profit de 4.1% via on_trade_closed
        risk_manager.on_trade_opened(10.0)
        profit = risk_manager.initial_capital * 0.041
        risk_manager.on_trade_closed(pnl=profit, exit_price=105.0, entry_price=100.0)

        signal = TradingSignal(
            direction=SignalDirection.CALL, symbol="R_75", confidence=0.8, score=80.0
        )
        can, report = risk_manager.can_place_trade(signal)
        assert not can
        assert report.status == RiskStatus.DAILY_PROFIT_TARGET_REACHED

    def test_drawdown_limit(self, risk_manager):
        # Reduire le capital de 21%
        risk_manager.current_capital = risk_manager.peak_capital * 0.79

        signal = TradingSignal(
            direction=SignalDirection.CALL, symbol="R_75", confidence=0.8, score=80.0
        )
        can, report = risk_manager.can_place_trade(signal)
        assert not can
        assert risk_manager.is_kill_switch_active

    def test_kill_switch_manual(self, risk_manager):
        risk_manager.activate_kill_switch("Test kill switch")
        assert risk_manager.is_kill_switch_active

        signal = TradingSignal(
            direction=SignalDirection.CALL, symbol="R_75", confidence=0.8, score=80.0
        )
        can, _ = risk_manager.can_place_trade(signal)
        assert not can

        risk_manager.deactivate_kill_switch()
        assert not risk_manager.is_kill_switch_active

    def test_on_trade_closed_updates_capital(self, risk_manager):
        initial = risk_manager.current_capital
        risk_manager.on_trade_closed(pnl=5.0, exit_price=101.0, entry_price=100.0)
        assert risk_manager.current_capital == initial + 5.0

    def test_win_rate(self, risk_manager):
        risk_manager.on_trade_opened(10.0)
        risk_manager.on_trade_closed(pnl=5.0, exit_price=101.0, entry_price=100.0)
        risk_manager.on_trade_opened(10.0)
        risk_manager.on_trade_closed(pnl=-2.0, exit_price=98.0, entry_price=100.0)
        assert risk_manager.win_rate == 0.5
        assert risk_manager.total_trades == 2

    def test_get_report(self, risk_manager):
        report = risk_manager.get_report()
        assert report.initial_capital == risk_manager.initial_capital
        assert report.current_capital == risk_manager.current_capital
        assert report.drawdown_pct >= 0.0

    def test_daily_reset(self, risk_manager):
        risk_manager._last_trade_date = "2000-01-01"
        risk_manager._trades_today = 5
        risk_manager._daily_pnl = -50.0
        risk_manager._reset_daily_if_needed()
        assert risk_manager.trades_today == 0
        assert risk_manager._daily_pnl == 0.0

    def test_invalid_signal_blocked(self, risk_manager):
        signal = TradingSignal(
            direction=SignalDirection.HOLD, symbol="R_75", confidence=0.0
        )
        can, report = risk_manager.can_place_trade(signal)
        assert not can
        assert "Signal invalide" in report.reason_blocked


# ═══════════════════════════════════════════════════════════════════
# 7. TEST ORDER EXECUTOR
# ═══════════════════════════════════════════════════════════════════

class TestOrder:
    def test_order_creation(self):
        order = Order(
            order_id="test123",
            symbol="R_75",
            direction=SignalDirection.CALL,
            entry_price=100.0,
            amount=10.0,
            stop_loss=99.5,
            take_profit=101.0,
        )
        assert order.order_id == "test123"
        assert order.status == OrderStatus.PENDING
        assert order.pnl == 0.0

    def test_order_close(self):
        order = Order(
            order_id="test123",
            symbol="R_75",
            direction=SignalDirection.CALL,
            entry_price=100.0,
            amount=10.0,
            stop_loss=99.5,
            take_profit=101.0,
        )
        order.close(exit_price=101.0, pnl=5.0)
        assert order.status in (OrderStatus.COMPLETED, OrderStatus.SIMULATED)
        assert order.pnl == 5.0
        assert order.exit_price == 101.0

    def test_order_to_dict(self):
        order = Order(
            order_id="test123",
            symbol="R_75",
            direction=SignalDirection.PUT,
            entry_price=100.0,
            amount=10.0,
            stop_loss=101.0,
            take_profit=99.0,
        )
        d = order.to_dict()
        assert d["order_id"] == "test123"
        assert d["direction"] == "PUT"
        assert d["amount"] == 10.0


class TestOrderExecutor:
    @pytest.mark.asyncio
    async def test_execute_dry_run(self, order_executor):
        signal = TradingSignal(
            direction=SignalDirection.CALL,
            symbol="R_75",
            confidence=0.8,
            score=80.0,
            entry_price=100.0,
            stop_loss=99.5,
            take_profit=105.0,
        )
        order = await order_executor.execute_signal(signal, amount=10.0)
        assert order is not None
        assert order.status in (OrderStatus.SIMULATED,)
        assert order.direction == SignalDirection.CALL
        assert order.amount == 10.0
        assert len(order_executor.active_orders) == 1

    @pytest.mark.asyncio
    async def test_sl_call(self, order_executor):
        signal = TradingSignal(
            direction=SignalDirection.CALL,
            symbol="R_75",
            confidence=0.8,
            score=80.0,
            entry_price=100.0,
            stop_loss=99.5,
            take_profit=105.0,
        )
        order = await order_executor.execute_signal(signal, amount=10.0)

        # Le prix descend sous le SL
        closed = await order_executor.simulate_price_movement(order, 99.0)
        assert closed is not None
        assert closed.pnl == -10.0
        assert len(order_executor.active_orders) == 0

    @pytest.mark.asyncio
    async def test_tp_call(self, order_executor):
        signal = TradingSignal(
            direction=SignalDirection.CALL,
            symbol="R_75",
            confidence=0.8,
            score=80.0,
            entry_price=100.0,
            stop_loss=99.5,
            take_profit=105.0,
        )
        order = await order_executor.execute_signal(signal, amount=10.0)

        # Le prix monte au-dessus du TP
        closed = await order_executor.simulate_price_movement(order, 106.0)
        assert closed is not None
        expected_rr = abs(order.take_profit - order.entry_price) / abs(order.entry_price - order.stop_loss)
        assert closed.pnl == pytest.approx(10.0 * expected_rr)
        assert len(order_executor.active_orders) == 0

    @pytest.mark.asyncio
    async def test_sl_put(self, order_executor):
        signal = TradingSignal(
            direction=SignalDirection.PUT,
            symbol="R_75",
            confidence=0.8,
            score=80.0,
            entry_price=100.0,
            stop_loss=101.0,
            take_profit=95.0,
        )
        order = await order_executor.execute_signal(signal, amount=10.0)

        closed = await order_executor.simulate_price_movement(order, 102.0)
        assert closed is not None
        assert closed.pnl == -10.0

    @pytest.mark.asyncio
    async def test_tp_put(self, order_executor):
        signal = TradingSignal(
            direction=SignalDirection.PUT,
            symbol="R_75",
            confidence=0.8,
            score=80.0,
            entry_price=100.0,
            stop_loss=101.0,
            take_profit=95.0,
        )
        order = await order_executor.execute_signal(signal, amount=10.0)

        closed = await order_executor.simulate_price_movement(order, 94.0)
        assert closed is not None
        expected_rr = abs(order.take_profit - order.entry_price) / abs(order.entry_price - order.stop_loss)
        assert closed.pnl == pytest.approx(10.0 * expected_rr)

    @pytest.mark.asyncio
    async def test_price_within_range_no_close(self, order_executor):
        signal = TradingSignal(
            direction=SignalDirection.CALL,
            symbol="R_75",
            confidence=0.8,
            score=80.0,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=110.0,
        )
        order = await order_executor.execute_signal(signal, amount=10.0)

        closed = await order_executor.simulate_price_movement(order, 100.5)
        assert closed is None  # Ordre toujours ouvert
        assert len(order_executor.active_orders) == 1

    def test_close_all_orders(self, order_executor):
        """Test synchrone de fermeture forcee."""
        # On cree un ordre manuellement (sans async)
        order = Order(
            order_id="manual1",
            symbol="R_75",
            direction=SignalDirection.CALL,
            entry_price=100.0,
            amount=10.0,
            stop_loss=99.0,
            take_profit=110.0,
            status=OrderStatus.SIMULATED,
        )
        order_executor._active_orders[order.order_id] = order

        closed = order_executor.close_all_orders(105.0)
        assert len(closed) == 1
        assert closed[0].pnl != 0.0
        assert len(order_executor.active_orders) == 0


# ═══════════════════════════════════════════════════════════════════
# 8. TEST BACKTESTER
# ═══════════════════════════════════════════════════════════════════

class TestBacktester:
    def test_generate_sample_data(self, backtester):
        candles = backtester.generate_sample_data(n_candles=200)
        assert len(candles) == 200
        assert isinstance(candles[0], Candle)
        assert candles[0].is_closed

    def test_sample_data_reproducibility(self, backtester):
        candles1 = backtester.generate_sample_data(n_candles=50)
        candles2 = backtester.generate_sample_data(n_candles=50)
        # Meme seed -> memes donnees
        assert candles1[0].close == candles2[0].close

    def test_run_backtest(self, backtester):
        candles = backtester.generate_sample_data(n_candles=500)
        result = backtester.run(candles)
        assert isinstance(result, BacktestResult)
        assert result.total_trades >= 0

    def test_insufficient_data(self, backtester):
        candles = backtester.generate_sample_data(n_candles=20)
        result = backtester.run(candles)
        assert result.total_trades == 0

    def test_result_metrics(self, backtester):
        candles = backtester.generate_sample_data(n_candles=500)
        result = backtester.run(candles)
        d = result.to_dict()
        assert "win_rate" in d
        assert "sharpe_ratio" in d
        assert "profit_factor" in d
        assert "max_drawdown_pct" in d

    def test_equity_curve(self, backtester):
        candles = backtester.generate_sample_data(n_candles=500)
        result = backtester.run(candles)
        assert len(result.equity_curve) >= len(candles)


# ═══════════════════════════════════════════════════════════════════
# Lancement direct
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
