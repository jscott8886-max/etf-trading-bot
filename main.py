# ETF Trading Bot - v4.1
# 5 Strategies: EMA + MSS + VPA + Breakout + Gap Fill Detector
# SPY + QQQ + GLD + SQQQ + IWM + DIA + XLF + XLK + TLT
# Gap fill logic: wait 10min, short the fill or buy the continuation
# No PDT limit | 1H candle lockout | Force close 3:55PM ET
import os, time, logging, math
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
import threading

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

API_KEY    = os.environ.get("ALPACA_API_KEY", "")
API_SECRET = os.environ.get("ALPACA_API_SECRET", "")
PAPER_MODE = os.environ.get("PAPER_MODE", "true").lower() == "true"

SYMBOLS    = ["SPY", "QQQ", "GLD", "SQQQ", "IWM", "DIA", "XLF", "XLK", "TLT"]
LONG_ONLY  = ["GLD", "SQQQ"]
SHORT_ELIG = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLK", "TLT"]
STRATEGIES = ["EMA", "MSS", "VPA", "Breakout", "Gap"]

# ── Strategy configs ───────────────────────────────────────────────────────────
EMA_CONFIG = {
    "name": "EMA",
    "rsi_hard_gate": 55,
    "rsi_entry_max": 40,
    "bb_min_bw": 0.3,
    "min_score": 4, "min_score_confirmed": 3,
    "atr_min_mult": 0.7,
    "volume_bonus_mult": 1.5,
    "time_filter": True,
}

MSS_CONFIG = {
    "name": "MSS",
    "swing_lookback": 10,
    "swing_fallback": 7,
    "fallback_hours": 2,
    "rsi_soft_threshold": 50,
    "atr_min_mult": 0.7,
    "volume_bonus_mult": 1.5,
    "time_filter": True,
}

VPA_CONFIG = {
    "name": "VPA",
    "volume_spike_mult": 2.5,
    "volume_avg_period": 20,
    "min_close_ratio": 0.6,
    "effort_result_ratio": 0.015,
    "min_score": 4,  # FIX: raised 3->4 to match crypto/forex confirmed threshold — weak scores were the problem there
    "bear_score_cap": 4,  # FIX: cap in bear regime — high VPA score in downtrend = distribution
    "time_filter": False,
}

BREAKOUT_CONFIG = {
    "name": "Breakout",
    "consolidation_candles": 8,
    "consolidation_threshold": 0.5,
    "breakout_volume_mult": 1.8,
    "breakout_candle_close_ratio": 0.6,
    "min_breakout_pct": 0.3,
    "time_filter": False,
}

GAP_CONFIG = {
    "name": "Gap",
    "min_gap_pct": 0.5,        # Min 0.5% gap to qualify
    "max_gap_pct": 3.0,         # Ignore extreme gaps > 3% (too risky)
    "volume_confirm_mult": 1.3, # Volume must be above average to confirm
    "entry_window_minutes": 30, # Only fire within first 30 min of market open
    "time_filter": True,
}

RISK = {
    "take_profit_pct": 1.5,
    "stop_loss_pct": 0.75,
    "position_size_with_trend": 0.25,
    "position_size_against_trend": 0.125,
    "max_long_positions": 6,
    "max_short_positions": 3,
    "short_lockout_minutes": 60,
    "last_short_entry_hour_et": 14,
    "force_close_hour_et": 15,
    "force_close_minute_et": 55,
    "vix_max": 30,
    "vix_reduce_threshold": 20,
    "min_score_long": 4,
    "min_score_short_bull": 4,
    "min_score_short_bear": 3,
    "long_only": LONG_ONLY,
    "short_eligible": SHORT_ELIG,
    "cooldown_minutes": 10, "time_exit_minutes": 30,
}

bot_state = {
    "running": True, "killed": False,
    "positions": {},
    "strategy_positions": {s: [] for s in STRATEGIES},
    "closed_trades": [], "diary": [],
    "day_pnl": 0.0, "daily_start_equity": 0.0,
    "total_trades": 0, "win_count": 0,
    "strategy_stats": {s: {"trades": 0, "wins": 0, "pnl": 0.0} for s in STRATEGIES},
    "long_stats":  {"trades": 0, "wins": 0, "pnl": 0.0},
    "short_stats": {"trades": 0, "wins": 0, "pnl": 0.0},
    "signals": {s: {} for s in ["SPY","QQQ","GLD","SQQQ","IWM","DIA","XLF","XLK","TLT"]},
    "account_cash": 0.0, "account_equity": 0.0, "account_buying_power": 0.0,
    "market_regime": "UNKNOWN",
    "vix": 0.0, "vix_status": "UNKNOWN",
    "short_lockouts": {},
    "active_cooldowns": {},
    "market_open": False,
    "in_trading_window": False,
    "daily_paused": False,
    "prev_closes": {},       # Previous day closes for gap detection
    "gap_fired_today": {},   # Track gap signals already fired today
    "mss_last_signal_time": {s: None for s in ["SPY","QQQ","GLD","SQQQ","IWM","DIA","XLF","XLK","TLT"]},
    "pending_confirmation": {},
    "version": "ETF-4.1"
}

# ── Alpaca helpers ─────────────────────────────────────────────────────────────
def get_trading_client():
    from alpaca.trading.client import TradingClient
    return TradingClient(api_key=API_KEY, secret_key=API_SECRET, paper=PAPER_MODE)

def get_data_client():
    from alpaca.data.historical import StockHistoricalDataClient
    return StockHistoricalDataClient(api_key=API_KEY, secret_key=API_SECRET)

def get_bars(symbol, timeframe="5Min", limit=100):
    try:
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        client = get_data_client()
        tf_map = {
            "1Min":  TimeFrame(1, TimeFrameUnit.Minute),
            "5Min":  TimeFrame(5, TimeFrameUnit.Minute),
            "15Min": TimeFrame(15, TimeFrameUnit.Minute),
            "1Hour": TimeFrame(1, TimeFrameUnit.Hour),
            "1Day":  TimeFrame(1, TimeFrameUnit.Day),
        }
        tf = tf_map.get(timeframe, TimeFrame(5, TimeFrameUnit.Minute))
        end = datetime.now(timezone.utc)
        if timeframe == "1Day":
            start = end - timedelta(days=limit + 10)
        elif timeframe == "1Hour":
            start = end - timedelta(hours=limit + 5)
        elif timeframe == "15Min":
            start = end - timedelta(minutes=limit * 20)
        else:
            start = end - timedelta(minutes=limit * 6)
        req = StockBarsRequest(symbol_or_symbols=symbol, timeframe=tf,
                               start=start, limit=limit, feed="iex")
        bars = get_data_client().get_stock_bars(req)
        df = bars.df
        if df.empty:
            return []
        if hasattr(df.index, 'levels'):
            df = df.loc[symbol] if symbol in df.index.get_level_values(0) else df
        result = []
        for idx, row in df.iterrows():
            result.append({
                "time": idx.isoformat() if hasattr(idx, 'isoformat') else str(idx),
                "open": float(row["open"]), "high": float(row["high"]),
                "low": float(row["low"]), "close": float(row["close"]),
                "volume": float(row["volume"])
            })
        return result[-limit:]
    except Exception as e:
        log.error(f"Bars error {symbol} {timeframe}: {e}")
        return []

def refresh_account():
    try:
        tc = get_trading_client()
        acct = tc.get_account()
        bot_state["account_cash"]         = float(acct.cash)
        bot_state["account_equity"]       = float(acct.equity)
        bot_state["account_buying_power"] = float(acct.buying_power)
        if bot_state["daily_start_equity"] == 0.0:
            bot_state["daily_start_equity"] = float(acct.equity)
    except Exception as e:
        log.error(f"Account refresh error: {e}")

def sync_positions():
    try:
        tc = get_trading_client()
        positions = tc.get_all_positions()
        synced = {}
        active_symbols = set()
        for p in positions:
            sym = p.symbol
            if sym not in SYMBOLS:
                continue
            active_symbols.add(sym)
            qty = float(p.qty)
            existing = bot_state["positions"].get(sym, {})
            synced[sym] = {
                "symbol": sym, "entry": float(p.avg_entry_price),
                "qty": abs(qty), "side": "short" if qty < 0 else "long",
                "current_price": float(p.current_price),
                "unrealized_pnl": float(p.unrealized_pl),
                "open_time": existing.get("open_time", datetime.now(timezone.utc).isoformat()),
                "open_date": existing.get("open_date", get_et_time().date().isoformat()),
                "strategy": existing.get("strategy", "UNKNOWN")
            }
        # Clear strategy slots for closed positions
        for strat in STRATEGIES:
            bot_state["strategy_positions"][strat] = [
                s for s in bot_state["strategy_positions"].get(strat, []) if s in active_symbols
            ]
        bot_state["positions"] = synced
    except Exception as e:
        log.error(f"Sync positions error: {e}")

def place_order(symbol, qty, side):
    try:
        from alpaca.trading.requests import MarketOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
        tc = get_trading_client()
        req = MarketOrderRequest(
            symbol=symbol, qty=round(qty, 2),
            side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
            time_in_force=TimeInForce.DAY
        )
        return tc.submit_order(req)
    except Exception as e:
        log.error(f"Order error {symbol} {side}: {e}")
        return None

def close_position_alpaca(symbol):
    try:
        tc = get_trading_client()
        tc.close_position(symbol)
        return True
    except Exception as e:
        log.error(f"Close error {symbol}: {e}")
        return False

def add_diary(symbol, text, entry_type="info", strategy="SYSTEM"):
    label = f"[{strategy}] " if strategy != "SYSTEM" else ""
    entry = {
        "time": datetime.now(timezone.utc).strftime("%H:%M"),
        "symbol": symbol, "text": f"{label}{text}",
        "type": entry_type, "strategy": strategy
    }
    bot_state["diary"].insert(0, entry)
    if len(bot_state["diary"]) > 300:
        bot_state["diary"] = bot_state["diary"][:300]

# ── Time helpers ───────────────────────────────────────────────────────────────
def get_et_time():
    try:
        import zoneinfo
        return datetime.now(zoneinfo.ZoneInfo("America/New_York"))
    except ImportError:
        try:
            import pytz
            return datetime.now(pytz.utc).astimezone(pytz.timezone("America/New_York"))
        except ImportError:
            utc_now = datetime.now(timezone.utc)
            et_offset = -4 if 3 <= utc_now.month <= 11 else -5
            return utc_now + timedelta(hours=et_offset)

def is_market_open():
    et = get_et_time()
    if et.weekday() >= 5:
        return False
    mins = et.hour * 60 + et.minute
    return 570 <= mins < 960  # 9:30AM to 4:00PM

def is_trading_window():
    et = get_et_time()
    if et.weekday() >= 5:
        return False
    mins = et.hour * 60 + et.minute
    return 570 <= mins < 930  # 9:30AM to 3:30PM

def can_open_short():
    et = get_et_time()
    if et.weekday() >= 5:
        return False
    mins = et.hour * 60 + et.minute
    return 570 <= mins < 840  # 9:30AM to 2:00PM

def is_within_open_window():
    """First 60 minutes after market open — gap detector waits 10min then trades"""
    et = get_et_time()
    if et.weekday() >= 5:
        return False
    mins = et.hour * 60 + et.minute
    return 570 <= mins < 630  # 9:30AM to 10:30AM

def is_gap_confirmation_ready():
    """Returns True after 9:40AM ET — 10 minutes after open for gap direction confirmation"""
    et = get_et_time()
    mins = et.hour * 60 + et.minute
    return mins >= 580  # 9:40AM ET

def should_force_close():
    et = get_et_time()
    return et.hour == 15 and et.minute >= 55 or et.hour >= 16

def is_short_locked_out(symbol):
    lockout_time = bot_state["short_lockouts"].get(symbol)
    if not lockout_time:
        return False
    minutes_elapsed = (datetime.now(timezone.utc) -
                      datetime.fromisoformat(lockout_time)).total_seconds() / 60
    if minutes_elapsed < RISK["short_lockout_minutes"]:
        return True
    lockout_dt = datetime.fromisoformat(lockout_time)
    next_hour = (lockout_dt + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return datetime.now(timezone.utc) < next_hour

def is_same_day_position(symbol):
    pos = bot_state["positions"].get(symbol, {})
    return pos.get("open_date", "") == get_et_time().date().isoformat()

# ── Indicators ─────────────────────────────────────────────────────────────────
def calc_ema(prices, period):
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for p in prices[period:]:
        ema.append(p * k + ema[-1] * (1 - k))
    return ema

def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag/al))

def calc_bb(closes, period=20, std_dev=2.0):
    if len(closes) < period:
        return None, None, None
    window = closes[-period:]
    mid = sum(window) / period
    std = math.sqrt(sum((x-mid)**2 for x in window) / period)
    return mid - std_dev*std, mid, mid + std_dev*std

def calc_atr(bars, period=14):
    if len(bars) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h = bars[i]["high"]; l = bars[i]["low"]; pc = bars[i-1]["close"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return sum(trs[-period:]) / period if len(trs) >= period else sum(trs)/len(trs)

# ── Market regime & VIX ────────────────────────────────────────────────────────
def check_market_regime():
    """SPY regime check with 200 EMA primary, 50 EMA fallback"""
    try:
        bars = get_bars("SPY", "1Day", 210)
        if not bars:
            log.warning("Regime check: no daily bars returned")
            return "UNKNOWN"

        closes = [b["close"] for b in bars]
        log.info(f"Regime check: got {len(closes)} daily bars for SPY")

        # Primary: 200 EMA
        if len(closes) >= 200:
            ema200 = calc_ema(closes, 200)
            if ema200:
                regime = "BULL" if closes[-1] > ema200[-1] else "BEAR"
                log.info(f"Regime: {regime} | SPY={closes[-1]:.2f} | 200EMA={ema200[-1]:.2f}")
                return regime

        # Fallback: 50 EMA if not enough bars for 200
        if len(closes) >= 50:
            ema50 = calc_ema(closes, 50)
            if ema50:
                regime = "BULL" if closes[-1] > ema50[-1] else "BEAR"
                log.info(f"Regime (50EMA fallback): {regime} | SPY={closes[-1]:.2f} | 50EMA={ema50[-1]:.2f}")
                return regime

        # Last resort: 20 EMA
        if len(closes) >= 20:
            ema20 = calc_ema(closes, 20)
            if ema20:
                regime = "BULL" if closes[-1] > ema20[-1] else "BEAR"
                log.info(f"Regime (20EMA fallback): {regime} | SPY={closes[-1]:.2f} | 20EMA={ema20[-1]:.2f}")
                return regime

        log.warning(f"Regime check: only {len(closes)} bars — cannot determine regime")
        return "UNKNOWN"
    except Exception as e:
        log.error(f"Regime error: {e}")
        import traceback
        log.error(traceback.format_exc())
        return "UNKNOWN"

def get_vix():
    try:
        bars = get_bars("VIXY", "1Day", 3)
        return bars[-1]["close"] if bars else 15.0
    except:
        return 15.0

def get_position_size(symbol, side, regime):
    """Calculate position size based on regime, VIX, and symbol type"""
    vix = bot_state["vix"]
    if side == "long":
        with_trend = regime == "BULL" or symbol in ["GLD"]
    else:
        with_trend = regime == "BEAR"

    # SQQQ benefits from bear regime
    if symbol == "SQQQ":
        with_trend = regime == "BEAR"

    size = RISK["position_size_with_trend"] if with_trend else RISK["position_size_against_trend"]

    # VIX adjustment
    if vix > RISK["vix_reduce_threshold"]:
        size = size * 0.5

    return size

def can_enter(symbol, strategy, side):
    if bot_state["killed"]: return False, "Kill switch"
    if bot_state["daily_paused"]: return False, "Daily loss limit"
    if bot_state["vix"] > RISK["vix_max"]: return False, f"VIX too high {bot_state['vix']}"

    # Strategy slot check — allow 2 per strategy
    strat_positions = bot_state["strategy_positions"].get(strategy, [])
    if len(strat_positions) >= 2:
        return False, f"{strategy} at max positions"

    # Symbol already held
    if symbol in bot_state["positions"]:
        return False, f"{symbol} already held"

    if side == "long":
        longs = [p for p in bot_state["positions"].values() if p["side"] == "long"]
        if len(longs) >= RISK["max_long_positions"]:
            return False, "Max long positions"
    else:
        if symbol in LONG_ONLY: return False, f"{symbol} is long only"
        shorts = [p for p in bot_state["positions"].values() if p["side"] == "short"]
        if len(shorts) >= RISK["max_short_positions"]:
            return False, "Max short positions"
        if is_short_locked_out(symbol): return False, "Short lockout active"
        if not can_open_short(): return False, "After 2PM ET"

    return True, "OK"

# ── STRATEGY A: EMA ────────────────────────────────────────────────────────────
def run_ema(symbol, regime, tf="5Min"):
    try:
        bars_5m = get_bars(symbol, tf, 80)
        bars_1h = get_bars(symbol, "1Hour", 60)
        if len(bars_5m) < 30 or len(bars_1h) < 20: return {}

        closes_5m = [b["close"] for b in bars_5m]
        closes_1h = [b["close"] for b in bars_1h]
        volumes   = [b["volume"] for b in bars_5m]
        price     = closes_5m[-1]

        if all(v == 0 for v in volumes[-5:]): return {}

        ema9   = calc_ema(closes_5m, 9)
        ema21  = calc_ema(closes_5m, 21)
        ema50  = calc_ema(closes_5m, 50)
        ema20_1h = calc_ema(closes_1h, 20)
        ema50_1h = calc_ema(closes_1h, 50)
        rsi      = calc_rsi(closes_5m)
        rsi_prev = calc_rsi(closes_5m[:-2])
        bb_low, bb_mid, bb_high = calc_bb(closes_5m)
        atr      = calc_atr(bars_5m)
        avg_atr  = calc_atr(bars_5m[:-10]) if len(bars_5m) > 15 else atr

        if not ema9 or not ema21 or not ema50_1h or bb_mid is None: return {}

        bb_bw     = ((bb_high - bb_low) / bb_mid) * 100 if bb_mid > 0 else 0
        avg_vol   = sum(volumes[-20:]) / 20
        vol_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 0
        rsi_rising  = rsi > rsi_prev
        rsi_falling = rsi < rsi_prev
        atr_ok = avg_atr == 0 or atr >= avg_atr * EMA_CONFIG["atr_min_mult"]

        # RSI hard gate for longs
        long_blocked = rsi > EMA_CONFIG["rsi_hard_gate"]
        short_blocked = rsi < (100 - EMA_CONFIG["rsi_hard_gate"])

        long_score = 0
        if not long_blocked:
            if price > ema50_1h[-1]:              long_score += 1
            if price > ema20_1h[-1]:              long_score += 1
            if ema9[-1] > ema21[-1]:              long_score += 2
            if len(ema9) > 1 and ema9[-1] > ema21[-1] and ema9[-2] <= ema21[-2]:
                long_score += 1
            if rsi < 40 and rsi_rising:           long_score += 2
            elif rsi < 50 and rsi_rising:         long_score += 1
            if bb_bw > EMA_CONFIG["bb_min_bw"] and price < bb_low:
                long_score += 1
            if vol_ratio >= EMA_CONFIG["volume_bonus_mult"]:
                long_score += 1

        short_score = 0
        if not short_blocked:
            if price < ema50_1h[-1]:              short_score += 1
            if price < ema20_1h[-1]:              short_score += 1
            if ema9[-1] < ema21[-1]:              short_score += 2
            if len(ema9) > 1 and ema9[-1] < ema21[-1] and ema9[-2] >= ema21[-2]:
                short_score += 1
            if rsi > 60 and rsi_falling:          short_score += 2
            elif rsi > 50 and rsi_falling:        short_score += 1
            if bb_bw > EMA_CONFIG["bb_min_bw"] and price > bb_high:
                short_score += 1
            if vol_ratio >= EMA_CONFIG["volume_bonus_mult"]:
                short_score += 1

        sig = {
            "price": price, "rsi": round(rsi,1),
            "rsi_rising": rsi_rising, "rsi_falling": rsi_falling,
            "bb_bw": round(bb_bw,2), "vol_ratio": round(vol_ratio,2),
            "atr_ok": atr_ok, "long_score": long_score, "short_score": short_score,
            "ema9": round(ema9[-1],2), "ema21": round(ema21[-1],2),
            "ema50_1h": round(ema50_1h[-1],2), "strategy": "EMA"
        }
        log.info(f"[EMA] {symbol} | price={price} RSI={round(rsi,1)} L={long_score} S={short_score}")
        return sig
    except Exception as e:
        log.error(f"[EMA] error {symbol}: {e}"); return {}

# ── STRATEGY B: MSS ────────────────────────────────────────────────────────────
def run_mss(symbol, regime, tf="5Min"):
    try:
        bars_5m = get_bars(symbol, tf, 80)
        bars_1h = get_bars(symbol, "1Hour", 30)
        if len(bars_5m) < 20 or len(bars_1h) < 15: return {}

        closes_5m = [b["close"] for b in bars_5m]
        closes_1h = [b["close"] for b in bars_1h]
        highs_1h  = [b["high"]  for b in bars_1h]
        lows_1h   = [b["low"]   for b in bars_1h]
        highs_5m  = [b["high"]  for b in bars_5m]
        lows_5m   = [b["low"]   for b in bars_5m]
        volumes   = [b["volume"] for b in bars_5m]
        price     = closes_5m[-1]

        if all(v == 0 for v in volumes[-5:]): return {}

        rsi     = calc_rsi(closes_5m)
        atr     = calc_atr(bars_5m)
        avg_atr = calc_atr(bars_5m[:-10]) if len(bars_5m) > 15 else atr
        avg_vol = sum(volumes[-20:]) / 20
        vol_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 0
        atr_ok = avg_atr == 0 or atr >= avg_atr * MSS_CONFIG["atr_min_mult"]

        # 1H trend
        rh = highs_1h[-5:]; ph = highs_1h[-10:-5]
        rl = lows_1h[-5:];  pl = lows_1h[-10:-5]
        trend_1h = "NEUTRAL"
        if rh and ph and rl and pl:
            if max(rh) > max(ph) and min(rl) > min(pl): trend_1h = "BULL"
            elif max(rh) < max(ph) and min(rl) < min(pl): trend_1h = "BEAR"

        # MSS lookback with fallback
        last_sig = bot_state["mss_last_signal_time"].get(symbol)
        if last_sig:
            hrs = (datetime.now(timezone.utc) - last_sig).total_seconds() / 3600
            lookback = MSS_CONFIG["swing_fallback"] if hrs > MSS_CONFIG["fallback_hours"] else MSS_CONFIG["swing_lookback"]
        else:
            lookback = MSS_CONFIG["swing_lookback"]

        recent_lows = lows_5m[-lookback:]
        recent_highs = highs_5m[-lookback:]

        # Bullish MSS: lower lows then higher low (reversal)
        bull_mss = (len(recent_lows) >= 5 and
                    recent_lows[-3] < recent_lows[-5] and
                    recent_lows[-1] > recent_lows[-2])

        # Bearish MSS: higher highs then lower high (reversal)
        bear_mss = (len(recent_highs) >= 5 and
                    recent_highs[-3] > recent_highs[-5] and
                    recent_highs[-1] < recent_highs[-2])

        if bull_mss or bear_mss:
            bot_state["mss_last_signal_time"][symbol] = datetime.now(timezone.utc)

        long_score = 0
        if bull_mss and trend_1h == "BULL":
            long_score += 3
            if rsi < MSS_CONFIG["rsi_soft_threshold"]: long_score += 1
            if vol_ratio >= MSS_CONFIG["volume_bonus_mult"]: long_score += 1

        short_score = 0
        if bear_mss and trend_1h == "BEAR":
            short_score += 3
            if rsi > (100 - MSS_CONFIG["rsi_soft_threshold"]): short_score += 1
            if vol_ratio >= MSS_CONFIG["volume_bonus_mult"]: short_score += 1

        sig = {
            "price": price, "trend_1h": trend_1h,
            "bull_mss": bull_mss, "bear_mss": bear_mss,
            "rsi": round(rsi,1), "vol_ratio": round(vol_ratio,2),
            "atr_ok": atr_ok, "lookback": lookback,
            "long_score": long_score, "short_score": short_score,
            "strategy": "MSS"
        }
        log.info(f"[MSS] {symbol} | trend={trend_1h} bullMSS={bull_mss} bearMSS={bear_mss} L={long_score} S={short_score}")
        return sig
    except Exception as e:
        log.error(f"[MSS] error {symbol}: {e}"); return {}

# ── STRATEGY C: VPA ────────────────────────────────────────────────────────────
def run_vpa(symbol, regime, tf="5Min"):
    try:
        bars = get_bars(symbol, tf, 40)
        if len(bars) < 25: return {}

        volumes = [b["volume"] for b in bars]
        closes  = [b["close"]  for b in bars]
        opens   = [b["open"]   for b in bars]
        highs   = [b["high"]   for b in bars]
        lows    = [b["low"]    for b in bars]

        if all(v == 0 for v in volumes[-5:]): return {}

        avg_vol    = sum(volumes[-VPA_CONFIG["volume_avg_period"]:]) / VPA_CONFIG["volume_avg_period"]
        curr_vol   = volumes[-1]
        vol_ratio  = curr_vol / avg_vol if avg_vol > 0 else 0
        price      = closes[-1]
        curr_open  = opens[-1]
        curr_close = closes[-1]
        curr_high  = highs[-1]
        curr_low   = lows[-1]
        bar_range  = curr_high - curr_low

        if bar_range == 0: return {}

        close_ratio = (curr_close - curr_low) / bar_range
        price_move  = bar_range / price if price > 0 else 0
        ema20 = calc_ema(closes, 20)

        long_score  = 0
        short_score = 0
        long_signals  = []
        short_signals = []

        # Bullish volume spike
        if vol_ratio >= VPA_CONFIG["volume_spike_mult"]:
            if close_ratio >= VPA_CONFIG["min_close_ratio"]:
                long_score += 2; long_signals.append("VOL_SPIKE_BULL")
            elif close_ratio <= (1 - VPA_CONFIG["min_close_ratio"]):
                short_score += 2; short_signals.append("VOL_SPIKE_BEAR")

        # Absorption
        if vol_ratio >= 2.5 and price_move < VPA_CONFIG["effort_result_ratio"]:
            if curr_close > curr_open:
                long_score += 2; long_signals.append("ABSORPTION_BULL")
            else:
                short_score += 2; short_signals.append("ABSORPTION_BEAR")

        # No supply / no demand
        if vol_ratio < 0.7 and curr_close > curr_open and close_ratio > 0.5:
            long_score += 1; long_signals.append("NO_SUPPLY")
        if vol_ratio < 0.7 and curr_close < curr_open and close_ratio < 0.5:
            short_score += 1; short_signals.append("NO_DEMAND")

        # Trend bonus
        if ema20 and price > ema20[-1]: long_score += 1
        if ema20 and price < ema20[-1]: short_score += 1

        # FIX: cap VPA long_score in bear regime — high score in downtrend = distribution not accumulation
        if regime == "BEAR" and long_score > VPA_CONFIG.get("bear_score_cap", 99):
            long_score = VPA_CONFIG["bear_score_cap"]
            long_signals.append("BEAR_CAPPED")

        sig = {
            "price": price, "vol_ratio": round(vol_ratio,2),
            "close_ratio": round(close_ratio,2),
            "long_score": long_score, "short_score": short_score,
            "long_signals": long_signals, "short_signals": short_signals,
            "strategy": "VPA"
        }
        log.info(f"[VPA] {symbol} | vol={round(vol_ratio,2)}x L={long_score} S={short_score} sigs={long_signals+short_signals}")
        return sig
    except Exception as e:
        log.error(f"[VPA] error {symbol}: {e}"); return {}

# ── STRATEGY D: BREAKOUT ───────────────────────────────────────────────────────
def run_breakout(symbol, regime, tf="5Min"):
    try:
        bars = get_bars(symbol, tf, 40)
        if len(bars) < 14: return {}

        closes  = [b["close"]  for b in bars]
        highs   = [b["high"]   for b in bars]
        lows    = [b["low"]    for b in bars]
        volumes = [b["volume"] for b in bars]
        opens   = [b["open"]   for b in bars]

        if all(v == 0 for v in volumes[-5:]): return {}

        price      = closes[-1]
        curr_open  = opens[-1]
        curr_close = closes[-1]
        curr_high  = highs[-1]
        curr_low   = lows[-1]
        curr_vol   = volumes[-1]
        avg_vol    = sum(volumes[-20:]) / len(volumes[-20:]) if volumes[-20:] else 1
        vol_ratio  = curr_vol / avg_vol if avg_vol > 0 else 0

        lookback = BREAKOUT_CONFIG["consolidation_candles"]
        if len(bars) < lookback + 2: return {}

        consol     = bars[-(lookback+2):-2]
        c_highs    = [b["high"] for b in consol]
        c_lows     = [b["low"]  for b in consol]
        c_high     = max(c_highs)
        c_low      = min(c_lows)
        c_range_pct = (c_high - c_low) / price * 100 if price > 0 else 0
        in_consol  = c_range_pct <= BREAKOUT_CONFIG["consolidation_threshold"]

        bar_range   = curr_high - curr_low
        close_ratio = (curr_close - curr_low) / bar_range if bar_range > 0 else 0
        bull_bo_pct = (curr_close - c_high) / c_high * 100 if c_high > 0 else 0
        bear_bo_pct = (c_low - curr_close) / c_low * 100 if c_low > 0 else 0

        # Confirmation candle
        prev = bars[-2] if len(bars) >= 2 else None
        prev_bull_confirmed = False
        prev_bear_confirmed = False
        if prev:
            pr = prev["high"] - prev["low"]
            if pr > 0:
                pcr = (prev["close"] - prev["low"]) / pr
                prev_bull_confirmed = prev["close"] > c_high and pcr >= 0.5
                prev_bear_confirmed = prev["close"] < c_low and pcr <= 0.5

        bull_breakout = (in_consol and curr_close > c_high and
                        bull_bo_pct >= BREAKOUT_CONFIG["min_breakout_pct"] and
                        vol_ratio >= BREAKOUT_CONFIG["breakout_volume_mult"] and
                        close_ratio >= BREAKOUT_CONFIG["breakout_candle_close_ratio"] and
                        prev_bull_confirmed)

        bear_breakout = (in_consol and curr_close < c_low and
                        bear_bo_pct >= BREAKOUT_CONFIG["min_breakout_pct"] and
                        vol_ratio >= BREAKOUT_CONFIG["breakout_volume_mult"] and
                        close_ratio <= (1 - BREAKOUT_CONFIG["breakout_candle_close_ratio"]) and
                        prev_bear_confirmed)

        long_score  = 4 if bull_breakout else 0
        short_score = 4 if bear_breakout else 0

        sig = {
            "price": price, "vol_ratio": round(vol_ratio,2),
            "consol_pct": round(c_range_pct,2), "in_consol": in_consol,
            "bull_breakout": bull_breakout, "bear_breakout": bear_breakout,
            "bull_bo_pct": round(bull_bo_pct,2), "bear_bo_pct": round(bear_bo_pct,2),
            "consol_high": round(c_high,2), "consol_low": round(c_low,2),
            "long_score": long_score, "short_score": short_score,
            "strategy": "Breakout"
        }
        log.info(f"[Breakout] {symbol} | vol={round(vol_ratio,1)}x consol={round(c_range_pct,2)}% bull={bull_breakout} bear={bear_breakout}")
        return sig
    except Exception as e:
        log.error(f"[Breakout] error {symbol}: {e}"); return {}

# ── STRATEGY E: GAP DETECTOR ───────────────────────────────────────────────────
def run_gap_detector(symbol, regime):
    """Gap Fill Strategy — waits 10 minutes after open to determine gap direction.
    
    Logic:
    1. Detect gap at 9:30AM (gap up or gap down vs yesterday's close)
    2. Wait until 9:40AM for 2 completed 5M candles
    3. If price is BELOW today's open → gap is filling → SHORT the fill (ride it down)
    4. If price is ABOVE today's open → gap is continuing → BUY the momentum
    5. GLD/SQQQ can't be shorted — skip gap-fill shorts on those
    """
    try:
        if not is_within_open_window():
            return {}

        # Already fired today for this symbol?
        today = get_et_time().date().isoformat()
        if bot_state["gap_fired_today"].get(symbol) == today:
            return {}

        # Get yesterday's close
        bars_daily = get_bars(symbol, "1Day", 5)
        if len(bars_daily) < 2: return {}

        prev_close = bars_daily[-2]["close"]
        bot_state["prev_closes"][symbol] = prev_close

        # Get today's 5M candles (need at least 2 completed = 10 minutes)
        bars_5m = get_bars(symbol, "5Min", 10)
        if not bars_5m or len(bars_5m) < 3: return {}

        today_open  = bars_5m[0]["open"]
        today_price = bars_5m[-1]["close"]
        curr_vol    = bars_5m[-1]["volume"]

        # Average volume from daily bars
        avg_vol = sum(b["volume"] for b in bars_daily[-5:]) / len(bars_daily[-5:])
        avg_5m_vol = avg_vol / 78  # ~78 five-minute candles per trading day
        vol_ratio  = curr_vol / avg_5m_vol if avg_5m_vol > 0 else 1

        gap_pct = (today_open - prev_close) / prev_close * 100
        is_gap = abs(gap_pct) >= GAP_CONFIG["min_gap_pct"] and abs(gap_pct) <= GAP_CONFIG["max_gap_pct"]
        vol_ok = vol_ratio >= GAP_CONFIG["volume_confirm_mult"]

        if not is_gap:
            return {}  # No significant gap today

        # Wait for 10-minute confirmation before deciding direction
        if not is_gap_confirmation_ready():
            log.info(f"[Gap] {symbol} | gap={round(gap_pct,2)}% detected — waiting for 9:40AM confirmation")
            return {}

        # Determine direction: is the gap filling or continuing?
        price_vs_open = today_price - today_open
        gap_filling = (gap_pct > 0 and price_vs_open < 0) or (gap_pct < 0 and price_vs_open > 0)
        gap_continuing = not gap_filling

        long_score = 0
        short_score = 0
        gap_action = ""

        if gap_filling:
            if gap_pct > 0:
                # Gap UP is filling — price falling back — SHORT the fill
                short_score = 5
                gap_action = "GAP_FILL_SHORT"
                log.info(f"[Gap] {symbol} | Gap UP {round(gap_pct,2)}% FILLING — price below open — SHORT signal")
            else:
                # Gap DOWN is filling — price rising back — BUY the fill
                long_score = 5
                gap_action = "GAP_FILL_LONG"
                log.info(f"[Gap] {symbol} | Gap DOWN {round(gap_pct,2)}% FILLING — price above open — LONG signal")
        else:
            if gap_pct > 0:
                # Gap UP continuing — price still above open — BUY momentum
                long_score = 5
                gap_action = "GAP_CONTINUE_LONG"
                log.info(f"[Gap] {symbol} | Gap UP {round(gap_pct,2)}% CONTINUING — price above open — LONG signal")
            else:
                # Gap DOWN continuing — price still below open — SHORT momentum
                short_score = 5
                gap_action = "GAP_CONTINUE_SHORT"
                log.info(f"[Gap] {symbol} | Gap DOWN {round(gap_pct,2)}% CONTINUING — price below open — SHORT signal")

        # Volume confirmation required
        if not vol_ok:
            long_score = 0
            short_score = 0

        sig = {
            "price": today_price,
            "prev_close": round(prev_close, 2),
            "today_open": round(today_open, 2),
            "gap_pct": round(gap_pct, 2),
            "vol_ratio": round(vol_ratio, 2),
            "gap_filling": gap_filling,
            "gap_continuing": gap_continuing,
            "gap_action": gap_action,
            "vol_ok": vol_ok,
            "long_score": long_score,
            "short_score": short_score,
            "strategy": "Gap"
        }
        return sig
    except Exception as e:
        log.error(f"[Gap] error {symbol}: {e}"); return {}

# ── EXIT HANDLER ───────────────────────────────────────────────────────────────
def check_exits(symbol, now, force_close=False):
    pos = bot_state["positions"].get(symbol)
    if not pos: return

    entry    = pos["entry"]
    qty      = pos["qty"]
    side     = pos["side"]
    strategy = pos.get("strategy", "UNKNOWN")

    # Get current price
    bars = get_bars(symbol, "1Min", 3)
    if not bars: return
    price = bars[-1]["close"]

    pct = (price - entry) / entry * 100 if side == "long" else (entry - price) / entry * 100

    should_exit = False
    reason = ""

    # FIX: 30-minute time exit — data showed losers rarely recover after 30min
    open_time_str = pos.get("open_time")
    minutes_open = 0
    if open_time_str:
        try:
            minutes_open = (now - datetime.fromisoformat(open_time_str)).total_seconds() / 60
        except: pass

    if force_close and is_same_day_position(symbol):
        should_exit = True; reason = "Force close 3:55PM ET"
    elif pct >= RISK["take_profit_pct"]:
        should_exit = True; reason = f"Take profit (+{round(pct,2)}%)"
    elif pct <= -RISK["stop_loss_pct"]:
        should_exit = True; reason = f"Stop loss ({round(pct,2)}%)"
        if side == "short":
            bot_state["short_lockouts"][symbol] = now.isoformat()
    elif minutes_open >= RISK.get("time_exit_minutes", 30) and pct < 0:
        should_exit = True; reason = f"30min time exit ({round(pct,2)}%)"
        if side == "short":
            bot_state["short_lockouts"][symbol] = now.isoformat()

    if should_exit:
        success = close_position_alpaca(symbol)
        if success:
            pnl = (price - entry) * qty if side == "long" else (entry - price) * qty
            win = pnl > 0

            bot_state["day_pnl"] += pnl
            bot_state["total_trades"] += 1
            if win: bot_state["win_count"] += 1

            # Update strategy stats
            if strategy in bot_state["strategy_stats"]:
                s = bot_state["strategy_stats"][strategy]
                s["trades"] += 1; s["pnl"] = round(s["pnl"] + pnl, 2)
                if win: s["wins"] += 1

            # Update side stats
            side_key = "long_stats" if side == "long" else "short_stats"
            s = bot_state[side_key]
            s["trades"] += 1; s["pnl"] = round(s["pnl"] + pnl, 2)
            if win: s["wins"] += 1

            # Clear strategy slot
            for strat in STRATEGIES:
                bot_state["strategy_positions"][strat] = [
                    s for s in bot_state["strategy_positions"].get(strat, []) if s != symbol
                ]

            add_diary(symbol,
                f"{'WIN' if win else 'LOSS'} [{side.upper()}] | "
                f"${entry:.2f}→${price:.2f} | "
                f"P&L ${round(pnl,2)} ({round(pct,2)}%) | {reason}",
                "win" if win else "loss", strategy)

            bot_state["closed_trades"].append({
                "symbol": symbol, "side": side, "strategy": strategy,
                "entry": entry, "exit": price,
                "pnl": round(pnl,2), "pct": round(pct,2),
                "win": win, "reason": reason,
                "time": now.strftime("%H:%M")
            })
            sync_positions()

# ── ENTRY HANDLER ──────────────────────────────────────────────────────────────
def try_entry(symbol, strategy, sig, regime, side, now):
    ok, reason = can_enter(symbol, strategy, side)
    if not ok:
        return

    min_score = RISK["min_score_long"] if side == "long" else (
        RISK["min_score_short_bear"] if regime == "BEAR" else RISK["min_score_short_bull"]
    )
    score_key = "long_score" if side == "long" else "short_score"
    score = sig.get(score_key, 0)

    if score < min_score: return
    if not sig.get("atr_ok", True) and strategy in ["EMA", "MSS"]: return

    # SQQQ and TLT profit from bear market — use short score
    if symbol in ["SQQQ", "TLT"] and side == "long":
        if sig.get("short_score", 0) < min_score: return

    # Time window checks
    if strategy in ["EMA", "MSS"] and not is_trading_window(): return
    if not is_trading_window() and strategy not in ["VPA", "Breakout", "Gap"]: return

    # Calculate position size
    size  = get_position_size(symbol, side, regime)
    cash  = bot_state["account_cash"]
    budget = cash * size
    price  = sig["price"]
    qty    = budget / price

    if budget < 50 or qty < 0.01: return

    order_side = "BUY" if side == "long" else "SELL"
    order = place_order(symbol, qty, order_side)

    if order:
        bot_state["positions"][symbol] = {
            "symbol": symbol, "entry": price, "qty": qty,
            "side": side, "current_price": price, "unrealized_pnl": 0,
            "open_time": now.isoformat(),
            "open_date": get_et_time().date().isoformat(),
            "strategy": strategy
        }
        if symbol not in bot_state["strategy_positions"].get(strategy, []):
            bot_state["strategy_positions"][strategy].append(symbol)

        # Mark gap as fired today
        if strategy == "Gap":
            bot_state["gap_fired_today"][symbol] = get_et_time().date().isoformat()

        sync_positions()
        add_diary(symbol,
            f"{'BUY' if side=='long' else 'SHORT'} | "
            f"${price:.2f} | Score {score} | "
            f"Size {round(size*100)}% | Regime {regime} | "
            f"VIX {bot_state['vix']:.1f}",
            "trade", strategy)
        log.info(f"[{strategy}] {side.upper()} {symbol} at {price} | score={score} | size={round(size*100)}%")

# ── TRADING LOOP ───────────────────────────────────────────────────────────────
def trading_loop():
    if not API_KEY or not API_SECRET:
        log.warning("No Alpaca credentials — cannot start")
        return

    add_diary("SYSTEM",
        "ETF Bot v4.0 started | SPY+QQQ+GLD+SQQQ+IWM+DIA+XLF+XLK+TLT | "
        "5M+15M scanning | VPA raised 3->4 + bear cap | 30min time exit | "
        "5 Strategies: EMA+MSS+VPA+Breakout+Gap | "
        "TP=1.5% SL=0.75% | 1H lockout | No PDT | "
        "Bear threshold=3 | Force close 3:55PM ET", "system")
    log.info("ETF Bot v4.1 started")

    regime_check_time = None
    vix_check_time    = None
    daily_reset_date  = None

    while True:
        try:
            now    = datetime.now(timezone.utc)
            et_now = get_et_time()
            today  = et_now.date()

            # Daily reset
            if daily_reset_date != today:
                bot_state["day_pnl"] = 0.0
                bot_state["daily_start_equity"] = 0.0
                bot_state["daily_paused"] = False
                bot_state["gap_fired_today"] = {}
                daily_reset_date = today
                log.info(f"Daily reset — {today}")

            if not is_market_open():
                bot_state["market_open"] = False
                bot_state["in_trading_window"] = False
                time.sleep(60)
                continue

            bot_state["market_open"] = True
            bot_state["in_trading_window"] = is_trading_window()

            refresh_account()
            sync_positions()

            # Force regime + VIX check on first loop after market opens
            if bot_state["market_regime"] == "UNKNOWN" or not regime_check_time:
                log.info("Forcing immediate regime check — first scan of the day")
                bot_state["market_regime"] = check_market_regime()
                regime_check_time = datetime.now(timezone.utc)
                # Also force VIX immediately
                vix = get_vix()
                bot_state["vix"] = round(vix, 2)
                bot_state["vix_status"] = "DANGER" if vix > RISK["vix_max"] else "ELEVATED" if vix > RISK["vix_reduce_threshold"] else "CALM"
                log.info(f"VIX (startup): {vix:.2f} — {bot_state['vix_status']}")
                vix_check_time = datetime.now(timezone.utc)

            # Check daily loss limit
            start_eq = bot_state["daily_start_equity"]
            if start_eq > 0:
                loss_pct = (start_eq - bot_state["account_equity"]) / start_eq * 100
                if loss_pct >= 5.0 and not bot_state["daily_paused"]:
                    bot_state["daily_paused"] = True
                    add_diary("SYSTEM", f"Daily loss limit 5% hit — paused", "system")

            # Force close at 3:55PM ET
            if should_force_close():
                for sym in list(bot_state["positions"].keys()):
                    check_exits(sym, now, force_close=True)
                time.sleep(60)
                continue

            # Regime check every 30 minutes
            if not regime_check_time or (now - regime_check_time).total_seconds() > 1800:
                bot_state["market_regime"] = check_market_regime()
                regime_check_time = now

            # VIX check every 15 minutes
            if not vix_check_time or (now - vix_check_time).total_seconds() > 900:
                vix = get_vix()
                bot_state["vix"] = round(vix, 2)
                bot_state["vix_status"] = "DANGER" if vix > RISK["vix_max"] else "ELEVATED" if vix > RISK["vix_reduce_threshold"] else "CALM"
                log.info(f"VIX: {vix:.2f} — {bot_state['vix_status']}")
                vix_check_time = now

            if bot_state["daily_paused"] or bot_state["killed"]:
                time.sleep(60)
                continue

            regime = bot_state["market_regime"]

            for symbol in SYMBOLS:
                if bot_state["killed"]: break

                # Exit checks first
                check_exits(symbol, now)

                if not is_trading_window(): continue

                # Run all 5 strategies in priority order
                # Gap (highest priority — time sensitive, only fires at open)
                if len(bot_state["strategy_positions"].get("Gap", [])) < 2:
                    sig = run_gap_detector(symbol, regime)
                    if sig:
                        if sig.get("long_score", 0) >= RISK["min_score_long"]:
                            try_entry(symbol, "Gap", sig, regime, "long", now)
                        elif sig.get("short_score", 0) >= RISK["min_score_short_bear"] and symbol in SHORT_ELIG:
                            try_entry(symbol, "Gap", sig, regime, "short", now)

                # Breakout, VPA, MSS, EMA — now scanning 5Min + 15Min (FIX: 15M timeframe added)
                for strat_name, run_fn, min_score_key in [
                    ("Breakout", run_breakout, None),
                    ("VPA", run_vpa, "VPA_MIN"),
                    ("MSS", run_mss, None),
                    ("EMA", run_ema, None),
                ]:
                    if len(bot_state["strategy_positions"].get(strat_name, [])) >= 2:
                        continue
                    for tf in ["5Min", "15Min"]:
                        sig = run_fn(symbol, regime, tf)
                        if not sig: continue
                        long_min = VPA_CONFIG["min_score"] if min_score_key else RISK["min_score_long"]
                        short_min = VPA_CONFIG["min_score"] if min_score_key else (
                            RISK["min_score_short_bear"] if regime=="BEAR" else RISK["min_score_short_bull"])
                        if sig.get("long_score", 0) >= long_min:
                            try_entry(symbol, strat_name, sig, regime, "long", now)
                        if sig.get("short_score", 0) >= short_min and symbol in SHORT_ELIG:
                            try_entry(symbol, strat_name, sig, regime, "short", now)
                        if len(bot_state["strategy_positions"].get(strat_name, [])) >= 2:
                            break

        except Exception as e:
            log.error(f"Loop error: {e}")
            import traceback
            log.error(traceback.format_exc())

        time.sleep(60)

threading.Thread(target=trading_loop, daemon=True).start()

# ── Flask routes ───────────────────────────────────────────────────────────────
@app.after_request
def no_cache(r):
    r.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    return r

def clean_nan(obj):
    if isinstance(obj, float):
        return 0.0 if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict): return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list): return [clean_nan(i) for i in obj]
    return obj

@app.route("/health")
def health():
    et = get_et_time()
    return jsonify({
        "status": "ok", "version": bot_state["version"],
        "time": datetime.now(timezone.utc).isoformat(),
        "et_time": et.strftime("%H:%M ET"),
        "market_open": bot_state["market_open"],
        "in_trading_window": bot_state["in_trading_window"],
        "regime": bot_state["market_regime"],
        "vix": bot_state["vix"], "vix_status": bot_state["vix_status"],
        "positions": len(bot_state["positions"]),
        "daily_paused": bot_state["daily_paused"]
    })

@app.route("/status")
def status():
    refresh_account()
    wins = bot_state["win_count"]; total = bot_state["total_trades"]
    et = get_et_time()
    return jsonify(clean_nan({
        "running": bot_state["running"], "killed": bot_state["killed"],
        "paper_mode": PAPER_MODE, "version": bot_state["version"],
        "market_open": bot_state["market_open"],
        "in_trading_window": bot_state["in_trading_window"],
        "et_time": et.strftime("%H:%M ET"),
        "positions": bot_state["positions"],
        "strategy_positions": bot_state["strategy_positions"],
        "closed_trades": bot_state["closed_trades"][-50:],
        "diary": bot_state["diary"][-100:],
        "day_pnl": bot_state["day_pnl"],
        "total_trades": total,
        "win_rate": round(wins/total*100) if total > 0 else 0,
        "strategy_stats": bot_state["strategy_stats"],
        "long_stats": bot_state["long_stats"],
        "short_stats": bot_state["short_stats"],
        "signals": bot_state["signals"],
        "account_cash": bot_state["account_cash"],
        "account_equity": bot_state["account_equity"],
        "account_buying_power": bot_state["account_buying_power"],
        "market_regime": bot_state["market_regime"],
        "vix": bot_state["vix"], "vix_status": bot_state["vix_status"],
        "short_lockouts": bot_state["short_lockouts"],
        "prev_closes": bot_state["prev_closes"],
        "gap_fired_today": bot_state["gap_fired_today"],
        "daily_paused": bot_state["daily_paused"],
        "strategy": RISK
    }))

@app.route("/diary")
def diary():
    strategy_filter = request.args.get("strategy")
    entries = bot_state["diary"]
    if strategy_filter:
        entries = [e for e in entries if e.get("strategy") == strategy_filter]
    return jsonify({"diary": entries})

@app.route("/kill", methods=["POST"])
def kill():
    bot_state["killed"] = not bot_state["killed"]
    status = "KILLED" if bot_state["killed"] else "RESUMED"
    add_diary("SYSTEM", f"Kill switch {status}", "system")
    return jsonify({"killed": bot_state["killed"]})

@app.route("/bars")
def bars():
    symbol = request.args.get("symbol", "SPY")
    tf     = request.args.get("timeframe", "5Min")
    data   = get_bars(symbol, tf, 150)
    result = []
    for b in data:
        try:
            from datetime import datetime as dt
            t = b["time"]
            ts = int(dt.fromisoformat(t.replace("Z","+00:00")).timestamp()) if isinstance(t,str) else int(t)
            result.append({"time": ts, "open": b["open"], "high": b["high"],
                           "low": b["low"], "close": b["close"]})
        except: pass
    return jsonify(result)

@app.route("/history")
def history():
    strategy_filter = request.args.get("strategy")
    trades = bot_state["closed_trades"]
    if strategy_filter:
        trades = [t for t in trades if t.get("strategy") == strategy_filter]
    return jsonify({"trades": trades})

@app.route("/stats")
def stats():
    return jsonify(clean_nan({
        "overall": {
            "total_trades": bot_state["total_trades"],
            "win_rate": round(bot_state["win_count"]/bot_state["total_trades"]*100)
                        if bot_state["total_trades"] > 0 else 0,
            "day_pnl": bot_state["day_pnl"]
        },
        "by_strategy": {
            s: {
                "trades": bot_state["strategy_stats"][s]["trades"],
                "wins": bot_state["strategy_stats"][s]["wins"],
                "win_rate": round(bot_state["strategy_stats"][s]["wins"] /
                            bot_state["strategy_stats"][s]["trades"] * 100)
                            if bot_state["strategy_stats"][s]["trades"] > 0 else 0,
                "pnl": bot_state["strategy_stats"][s]["pnl"]
            } for s in STRATEGIES
        }
    }))

@app.route("/")
def index():
    try:
        with open("index.html") as f: return f.read()
    except:
        return jsonify({"status": "ETF Bot v2.0 running",
                        "strategies": STRATEGIES, "symbols": SYMBOLS})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
