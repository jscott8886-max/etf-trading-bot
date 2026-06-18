# ETF Trading Bot - v1.0
# Long + Short on SPY and QQQ
# PDT protection, 1 short/day, 24hr lockout, auto-close by 3:55PM ET
import os, time, logging, math, json
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
 
SYMBOLS = ["SPY", "QQQ"]
 
STRATEGY = {
    # Risk
    "take_profit_pct": 1.5,
    "stop_loss_pct": 0.75,
    "position_size_with_trend": 0.25,    # 25% when trading with regime
    "position_size_against_trend": 0.125, # 12.5% when trading against regime
    "max_long_positions": 2,              # Can hold SPY and QQQ long simultaneously
    "max_short_positions": 1,             # Only 1 short at a time
    # Short rules
    "short_lockout_hours": 24,            # 24hr lockout after closing a short
    "last_short_entry_hour_et": 14,       # No new shorts after 2PM ET
    "force_close_hour_et": 15,            # Force close all same-day positions at 3:55PM ET
    "force_close_minute_et": 55,
    # PDT protection
    "max_day_trades_per_week": 3,
    # VIX filter
    "vix_max": 30,                        # No trades if VIX above 30
    "vix_reduce_threshold": 20,           # Reduce size if VIX above 20
    # Signal thresholds
    "min_score_long": 4,
    "min_score_short": 4,
    # Cooldown
    "cooldown_minutes": 30,
}
 
bot_state = {
    "running": True, "killed": False,
    "positions": {},                      # symbol -> position dict
    "long_positions": {},                 # symbol -> long position
    "short_positions": {},                # symbol -> short position
    "closed_trades": [], "diary": [],
    "day_pnl": 0.0, "daily_start_equity": 0.0,
    "total_trades": 0, "win_count": 0,
    "long_stats": {"trades": 0, "wins": 0, "pnl": 0.0},
    "short_stats": {"trades": 0, "wins": 0, "pnl": 0.0},
    "signals": {s: {} for s in SYMBOLS},
    "account_cash": 0.0, "account_equity": 0.0, "account_buying_power": 0.0,
    "market_regime": "UNKNOWN",           # Based on SPY 200 EMA
    "vix": 0.0,
    "vix_status": "UNKNOWN",
    "day_trades_used": 0,                 # Rolling count this week
    "day_trades_reset_date": None,
    "short_lockouts": {},                 # symbol -> datetime of last short close
    "same_day_shorts": {},                # symbol -> open date (to track PDT)
    "active_cooldowns": {},
    "market_open": False,
    "in_trading_window": False,
    "daily_paused": False,
    "version": "ETF-1.1"
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
        if timeframe == "1Min":
            tf = TimeFrame(1, TimeFrameUnit.Minute)
        elif timeframe == "5Min":
            tf = TimeFrame(5, TimeFrameUnit.Minute)
        elif timeframe == "1Hour":
            tf = TimeFrame(1, TimeFrameUnit.Hour)
        elif timeframe == "1Day":
            tf = TimeFrame(1, TimeFrameUnit.Day)
        else:
            tf = TimeFrame(5, TimeFrameUnit.Minute)
 
        end = datetime.now(timezone.utc)
        if timeframe == "1Day":
            start = end - timedelta(days=limit + 10)
        elif timeframe == "1Hour":
            start = end - timedelta(hours=limit + 5)
        else:
            start = end - timedelta(minutes=limit * 6)
 
        req = StockBarsRequest(symbol_or_symbols=symbol, timeframe=tf,
                               start=start, limit=limit,
                               feed="iex")
        bars = client.get_stock_bars(req)
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
        log.error(f"Bars error {symbol}: {e}")
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
        for p in positions:
            sym = p.symbol
            if sym not in SYMBOLS:
                continue
            qty = float(p.qty)
            synced[sym] = {
                "symbol": sym,
                "entry": float(p.avg_entry_price),
                "qty": abs(qty),
                "side": "short" if qty < 0 else "long",
                "current_price": float(p.current_price),
                "unrealized_pnl": float(p.unrealized_pl),
                "open_time": bot_state["positions"].get(sym, {}).get("open_time",
                             datetime.now(timezone.utc).isoformat()),
                "open_date": bot_state["positions"].get(sym, {}).get("open_date",
                             datetime.now(timezone.utc).date().isoformat())
            }
        bot_state["positions"] = synced
    except Exception as e:
        log.error(f"Sync positions error: {e}")
 
def place_order(symbol, qty, side):
    try:
        from alpaca.trading.requests import MarketOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
        tc = get_trading_client()
        req = MarketOrderRequest(
            symbol=symbol,
            qty=round(qty, 2),
            side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
            time_in_force=TimeInForce.DAY
        )
        return tc.submit_order(req)
    except Exception as e:
        log.error(f"Order error {symbol} {side}: {e}")
        return None
 
def close_position(symbol, side):
    try:
        tc = get_trading_client()
        tc.close_position(symbol)
        return True
    except Exception as e:
        log.error(f"Close error {symbol}: {e}")
        return False
 
def add_diary(symbol, text, entry_type="info"):
    entry = {
        "time": datetime.now(timezone.utc).strftime("%H:%M"),
        "symbol": symbol, "text": text, "type": entry_type
    }
    bot_state["diary"].insert(0, entry)
    if len(bot_state["diary"]) > 200:
        bot_state["diary"] = bot_state["diary"][:200]
 
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
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    if al == 0:
        return 100.0
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
 
# ── Market state ───────────────────────────────────────────────────────────────
def get_et_time():
    """Get current Eastern Time"""
    from datetime import timezone as tz
    utc_now = datetime.now(timezone.utc)
    # ET is UTC-5 (EST) or UTC-4 (EDT)
    # Simple approximation — use UTC-4 for EDT (March-November)
    et_offset = -4
    et_now = utc_now + timedelta(hours=et_offset)
    return et_now
 
def is_market_open():
    """US stock market hours: 9:30AM - 4:00PM ET, Mon-Fri"""
    et = get_et_time()
    wd = et.weekday()  # 0=Mon, 4=Fri, 5=Sat, 6=Sun
    if wd >= 5:
        log.info(f"Market closed — weekend (weekday={wd})")
        return False
    market_open_mins  = 9 * 60 + 30   # 9:30AM
    market_close_mins = 16 * 60        # 4:00PM
    current_mins = et.hour * 60 + et.minute
    is_open = market_open_mins <= current_mins < market_close_mins
    log.info(f"Market check | ET={et.strftime('%H:%M')} | mins={current_mins} | open={is_open} | wd={wd}")
    return is_open
 
def is_trading_window():
    """Can open new positions: 9:30AM - 3:30PM ET"""
    et = get_et_time()
    wd = et.weekday()
    if wd >= 5:
        return False
    open_mins  = 9 * 60 + 30   # 9:30AM
    close_mins = 15 * 60 + 30  # 3:30PM
    current_mins = et.hour * 60 + et.minute
    return open_mins <= current_mins < close_mins
 
def can_open_short():
    """Can open new shorts: 9:30AM - 2:00PM ET"""
    et = get_et_time()
    wd = et.weekday()
    if wd >= 5:
        return False
    open_mins  = 9 * 60 + 30   # 9:30AM
    close_mins = 14 * 60        # 2:00PM
    current_mins = et.hour * 60 + et.minute
    return open_mins <= current_mins < close_mins
 
def should_force_close():
    """Force close same-day positions at 3:55PM ET"""
    et = get_et_time()
    return et.hour == 15 and et.minute >= 55 or et.hour >= 16
 
def get_week_start():
    """Get Monday of current week"""
    et = get_et_time()
    days_since_monday = et.weekday()
    monday = et - timedelta(days=days_since_monday)
    return monday.date()
 
def check_and_reset_day_trades():
    """Reset PDT counter on Monday"""
    week_start = get_week_start()
    if bot_state["day_trades_reset_date"] != str(week_start):
        bot_state["day_trades_used"] = 0
        bot_state["day_trades_reset_date"] = str(week_start)
        log.info(f"PDT counter reset — week of {week_start}")
 
def is_short_locked_out(symbol):
    """Check 24hr lockout for a symbol"""
    lockout_time = bot_state["short_lockouts"].get(symbol)
    if not lockout_time:
        return False
    hours_elapsed = (datetime.now(timezone.utc) -
                    datetime.fromisoformat(lockout_time)).total_seconds() / 3600
    return hours_elapsed < STRATEGY["short_lockout_hours"]
 
def is_same_day_short(symbol):
    """Check if current short was opened today"""
    pos = bot_state["positions"].get(symbol, {})
    if pos.get("side") != "short":
        return False
    open_date = pos.get("open_date", "")
    today = get_et_time().date().isoformat()
    return open_date == today
 
def check_market_regime():
    """SPY 200-day EMA as market regime"""
    try:
        bars = get_bars("SPY", "1Day", 210)
        if len(bars) < 200:
            return "UNKNOWN"
        closes = [b["close"] for b in bars]
        ema200 = calc_ema(closes, 200)
        if not ema200:
            return "UNKNOWN"
        regime = "BULL" if closes[-1] > ema200[-1] else "BEAR"
        log.info(f"Market regime: {regime} | SPY={closes[-1]:.2f} | 200EMA={ema200[-1]:.2f}")
        return regime
    except Exception as e:
        log.error(f"Regime check error: {e}")
        return "UNKNOWN"
 
def get_vix():
    """Get VIX level"""
    try:
        bars = get_bars("VIXY", "1Day", 3)  # VIXY is VIX ETF on Alpaca
        if bars:
            vix = bars[-1]["close"]
            log.info(f"VIX proxy (VIXY): {vix:.2f}")
            return vix
        return 15.0  # Default to calm if can't fetch
    except Exception as e:
        log.error(f"VIX error: {e}")
        return 15.0
 
# ── Signal generation ──────────────────────────────────────────────────────────
def generate_signal(symbol):
    """Generate long and short signals for SPY/QQQ"""
    try:
        bars_5m = get_bars(symbol, "5Min", 80)
        bars_1h = get_bars(symbol, "1Hour", 60)
        if len(bars_5m) < 30 or len(bars_1h) < 30:
            return {}
 
        closes_5m = [b["close"] for b in bars_5m]
        closes_1h = [b["close"] for b in bars_1h]
        volumes   = [b["volume"] for b in bars_5m]
        price     = closes_5m[-1]
 
        # Stale data check
        if all(v == 0 for v in volumes[-5:]):
            return {}
 
        ema9  = calc_ema(closes_5m, 9)
        ema21 = calc_ema(closes_5m, 21)
        ema50 = calc_ema(closes_5m, 50)
        ema20_1h = calc_ema(closes_1h, 20)
        ema50_1h = calc_ema(closes_1h, 50)
        rsi   = calc_rsi(closes_5m)
        rsi_prev = calc_rsi(closes_5m[:-2])
        bb_low, bb_mid, bb_high = calc_bb(closes_5m)
        atr   = calc_atr(bars_5m)
        avg_atr = calc_atr(bars_5m[:-10]) if len(bars_5m) > 15 else atr
 
        if not ema9 or not ema21 or not ema50 or not ema20_1h or bb_mid is None:
            return {}
 
        bb_bw = ((bb_high - bb_low) / bb_mid) * 100 if bb_mid > 0 else 0
        avg_vol = sum(volumes[-20:]) / 20
        vol_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 0
        rsi_rising  = rsi > rsi_prev
        rsi_falling = rsi < rsi_prev
 
        # ATR filter — skip if market too quiet
        atr_ok = avg_atr == 0 or atr >= avg_atr * 0.7
 
        # ── LONG score ──────────────────────────────────────────────────────
        long_score = 0
        if price > ema50_1h[-1]:             long_score += 1  # Above 1H 50 EMA
        if price > ema20_1h[-1]:             long_score += 1  # Above 1H 20 EMA
        if ema9[-1] > ema21[-1]:             long_score += 2  # 5M EMA bullish
        if len(ema9) > 1 and ema9[-1] > ema21[-1] and ema9[-2] <= ema21[-2]:
            long_score += 1                                    # Fresh crossover
        if rsi < 40 and rsi_rising:          long_score += 2  # Oversold + rising
        elif rsi < 50 and rsi_rising:        long_score += 1  # Rising momentum
        if bb_bw > 0.3 and price < bb_low:  long_score += 1  # Below lower BB
        if vol_ratio >= 1.5:                 long_score += 1  # Volume confirmation
 
        # ── SHORT score ─────────────────────────────────────────────────────
        short_score = 0
        if price < ema50_1h[-1]:             short_score += 1  # Below 1H 50 EMA
        if price < ema20_1h[-1]:             short_score += 1  # Below 1H 20 EMA
        if ema9[-1] < ema21[-1]:             short_score += 2  # 5M EMA bearish
        if len(ema9) > 1 and ema9[-1] < ema21[-1] and ema9[-2] >= ema21[-2]:
            short_score += 1                                    # Fresh bearish crossover
        if rsi > 60 and rsi_falling:         short_score += 2  # Overbought + falling
        elif rsi > 50 and rsi_falling:       short_score += 1  # Falling momentum
        if bb_bw > 0.3 and price > bb_high:  short_score += 1  # Above upper BB
        if vol_ratio >= 1.5:                 short_score += 1  # Volume confirmation
 
        sig = {
            "price": price, "rsi": round(rsi, 1),
            "rsi_rising": rsi_rising, "rsi_falling": rsi_falling,
            "bb_bw": round(bb_bw, 2), "vol_ratio": round(vol_ratio, 2),
            "atr": round(atr, 4), "atr_ok": atr_ok,
            "ema9": round(ema9[-1], 2), "ema21": round(ema21[-1], 2),
            "ema50_1h": round(ema50_1h[-1], 2),
            "long_score": long_score, "short_score": short_score,
        }
        bot_state["signals"][symbol] = sig
        log.info(f"{symbol} | price={price} RSI={round(rsi,1)} "
                 f"LONG={long_score} SHORT={short_score} vol={round(vol_ratio,1)}x")
        return sig
 
    except Exception as e:
        log.error(f"Signal error {symbol}: {e}")
        return {}
 
# ── Exit handler ───────────────────────────────────────────────────────────────
def check_exits(symbol, price, now, et_now, force_close=False):
    pos = bot_state["positions"].get(symbol)
    if not pos:
        return
 
    entry = pos["entry"]
    qty   = pos["qty"]
    side  = pos["side"]
 
    if side == "long":
        pct = (price - entry) / entry * 100
    else:
        pct = (entry - price) / entry * 100  # Profit when price falls
 
    should_exit = False
    reason = ""
 
    # Force close same-day shorts at 3:55PM ET
    if force_close and side == "short" and is_same_day_short(symbol):
        should_exit = True
        reason = "Force close 3:55PM ET"
 
    # Take profit
    elif pct >= STRATEGY["take_profit_pct"]:
        should_exit = True
        reason = f"Take profit (+{round(pct,2)}%)"
 
    # Stop loss
    elif pct <= -STRATEGY["stop_loss_pct"]:
        should_exit = True
        reason = f"Stop loss ({round(pct,2)}%)"
 
    if should_exit:
        success = close_position(symbol, side)
        if success:
            pnl = (price - entry) * qty if side == "long" else (entry - price) * qty
            win = pnl > 0
 
            # Track PDT for same-day shorts
            if side == "short" and is_same_day_short(symbol):
                bot_state["day_trades_used"] += 1
                bot_state["short_lockouts"][symbol] = now.isoformat()
                log.info(f"Day trade used — {bot_state['day_trades_used']}/3 this week")
 
            bot_state["day_pnl"] += pnl
            bot_state["total_trades"] += 1
            if win:
                bot_state["win_count"] += 1
 
            stats_key = "long_stats" if side == "long" else "short_stats"
            bot_state[stats_key]["trades"] += 1
            bot_state[stats_key]["pnl"] = round(bot_state[stats_key]["pnl"] + pnl, 2)
            if win:
                bot_state[stats_key]["wins"] += 1
 
            entry_type = "win" if win else "loss"
            add_diary(symbol,
                f"{'WIN' if win else 'LOSS'} [{side.upper()}] | "
                f"${entry:.2f} → ${price:.2f} | "
                f"P&L ${round(pnl,2)} ({round(pct,2)}%) | {reason}",
                entry_type)
 
            bot_state["closed_trades"].append({
                "symbol": symbol, "side": side,
                "entry": entry, "exit": price,
                "pnl": round(pnl,2), "pct": round(pct,2),
                "win": win, "reason": reason,
                "time": now.strftime("%H:%M")
            })
            sync_positions()
 
# ── Entry handler ──────────────────────────────────────────────────────────────
def try_long(symbol, sig, regime, now):
    """Try to open a long position"""
    if not sig or sig.get("long_score", 0) < STRATEGY["min_score_long"]:
        return
    if not sig.get("atr_ok", True):
        return
    if symbol in bot_state["positions"] and bot_state["positions"][symbol]["side"] == "long":
        return  # Already long this symbol
    if len([p for p in bot_state["positions"].values() if p["side"] == "long"]) >= STRATEGY["max_long_positions"]:
        return  # Max long positions reached
    if bot_state["killed"]:
        return
    if not is_trading_window():
        return
 
    # VIX check
    if bot_state["vix"] > STRATEGY["vix_max"]:
        return
 
    # Position sizing based on regime
    size = STRATEGY["position_size_with_trend"] if regime != "BEAR" else STRATEGY["position_size_against_trend"]
 
    # Reduce size if VIX elevated
    if bot_state["vix"] > STRATEGY["vix_reduce_threshold"]:
        size = size * 0.5
 
    cash  = bot_state["account_cash"]
    budget = cash * size
    price  = sig["price"]
    qty    = budget / price
 
    if budget < 100 or qty < 0.01:
        return
 
    order = place_order(symbol, qty, "BUY")
    if order:
        bot_state["positions"][symbol] = {
            "symbol": symbol, "entry": price, "qty": qty,
            "side": "long", "current_price": price, "unrealized_pnl": 0,
            "open_time": now.isoformat(),
            "open_date": get_et_time().date().isoformat()
        }
        sync_positions()
        add_diary(symbol,
            f"LONG | ${price:.2f} | Score {sig['long_score']} | "
            f"RSI {sig['rsi']} | Vol {sig['vol_ratio']}x | "
            f"Size {round(size*100)}% | Regime {regime}",
            "trade")
        log.info(f"LONG {symbol} at {price} | score={sig['long_score']} | regime={regime}")
 
def try_short(symbol, sig, regime, now):
    """Try to open a short position"""
    if not sig or sig.get("short_score", 0) < STRATEGY["min_score_short"]:
        return
    if not sig.get("atr_ok", True):
        return
    if symbol in bot_state["positions"] and bot_state["positions"][symbol]["side"] == "short":
        return  # Already short this symbol
    if len([p for p in bot_state["positions"].values() if p["side"] == "short"]) >= STRATEGY["max_short_positions"]:
        return  # Max 1 short at a time
    if bot_state["killed"]:
        return
    if not can_open_short():
        return  # After 2PM ET — no new shorts
    if is_short_locked_out(symbol):
        return  # 24hr lockout
    if bot_state["day_trades_used"] >= STRATEGY["max_day_trades_per_week"]:
        log.info(f"PDT limit reached — {bot_state['day_trades_used']}/3 day trades used this week")
        return
 
    # VIX check
    if bot_state["vix"] > STRATEGY["vix_max"]:
        return
 
    # Position sizing based on regime
    size = STRATEGY["position_size_with_trend"] if regime == "BEAR" else STRATEGY["position_size_against_trend"]
 
    # Reduce size if VIX elevated
    if bot_state["vix"] > STRATEGY["vix_reduce_threshold"]:
        size = size * 0.5
 
    cash   = bot_state["account_cash"]
    budget = cash * size
    price  = sig["price"]
    qty    = budget / price
 
    if budget < 100 or qty < 0.01:
        return
 
    order = place_order(symbol, qty, "SELL")
    if order:
        bot_state["positions"][symbol] = {
            "symbol": symbol, "entry": price, "qty": qty,
            "side": "short", "current_price": price, "unrealized_pnl": 0,
            "open_time": now.isoformat(),
            "open_date": get_et_time().date().isoformat()
        }
        sync_positions()
        add_diary(symbol,
            f"SHORT | ${price:.2f} | Score {sig['short_score']} | "
            f"RSI {sig['rsi']} | Vol {sig['vol_ratio']}x | "
            f"Size {round(size*100)}% | Regime {regime} | "
            f"DayTrades {bot_state['day_trades_used']+1}/3",
            "trade")
        log.info(f"SHORT {symbol} at {price} | score={sig['short_score']} | regime={regime}")
 
# ── Trading loop ───────────────────────────────────────────────────────────────
def trading_loop():
    if not API_KEY or not API_SECRET:
        log.warning("No Alpaca credentials — cannot start")
        return
 
    add_diary("SYSTEM",
        "ETF Bot v1.0 started | SPY + QQQ | Long + Short | "
        "TP=1.5% SL=0.75% | 1 short/day | 24hr lockout | "
        "PDT protection 3/week | Force close 3:55PM ET",
        "system")
    log.info("ETF Bot v1.0 started")
 
    regime_check_time = None
    vix_check_time    = None
    daily_reset_date  = None
 
    while True:
        try:
            now    = datetime.now(timezone.utc)
            et_now = get_et_time()
 
            # Daily reset
            today = et_now.date()
            if daily_reset_date != today:
                bot_state["day_pnl"] = 0.0
                bot_state["daily_start_equity"] = 0.0
                bot_state["daily_paused"] = False
                bot_state["same_day_shorts"] = {}
                daily_reset_date = today
                log.info(f"Daily reset — {today}")
 
            # PDT weekly reset
            check_and_reset_day_trades()
 
            if not is_market_open():
                bot_state["market_open"] = False
                bot_state["in_trading_window"] = False
                time.sleep(60)
                continue
 
            bot_state["market_open"] = True
            bot_state["in_trading_window"] = is_trading_window()
 
            refresh_account()
            sync_positions()
 
            # Force close check at 3:55PM ET
            force_close = should_force_close()
            if force_close:
                for symbol in list(bot_state["positions"].keys()):
                    pos = bot_state["positions"].get(symbol, {})
                    if pos.get("side") == "short" and is_same_day_short(symbol):
                        bars = get_bars(symbol, "1Min", 3)
                        price = bars[-1]["close"] if bars else pos["entry"]
                        check_exits(symbol, price, now, et_now, force_close=True)
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
                if vix > STRATEGY["vix_max"]:
                    bot_state["vix_status"] = "DANGER"
                elif vix > STRATEGY["vix_reduce_threshold"]:
                    bot_state["vix_status"] = "ELEVATED"
                else:
                    bot_state["vix_status"] = "CALM"
                log.info(f"VIX: {vix:.2f} — {bot_state['vix_status']}")
                vix_check_time = now
 
            regime = bot_state["market_regime"]
 
            for symbol in SYMBOLS:
                if bot_state["killed"]:
                    break
 
                # Get signal
                sig = generate_signal(symbol)
                if not sig:
                    continue
 
                price = sig["price"]
 
                # Check exits
                check_exits(symbol, price, now, et_now)
 
                # Try entries if in trading window
                if is_trading_window():
                    # Long entry
                    if symbol not in bot_state["positions"] or \
                       bot_state["positions"][symbol]["side"] != "long":
                        try_long(symbol, sig, regime, now)
 
                    # Short entry
                    if symbol not in bot_state["positions"] or \
                       bot_state["positions"][symbol]["side"] != "short":
                        try_short(symbol, sig, regime, now)
 
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
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(i) for i in obj]
    try:
        import numpy as np
        if isinstance(obj, (np.floating, np.integer)):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
    except ImportError:
        pass
    return obj
 
@app.route("/health")
def health():
    et = get_et_time()
    return jsonify({
        "status": "ok",
        "time": datetime.now(timezone.utc).isoformat(),
        "et_time": et.strftime("%H:%M ET"),
        "version": bot_state["version"],
        "market_open": bot_state["market_open"],
        "in_trading_window": bot_state["in_trading_window"],
        "vix": bot_state["vix"],
        "vix_status": bot_state["vix_status"],
        "regime": bot_state["market_regime"],
        "day_trades_used": bot_state["day_trades_used"]
    })
 
@app.route("/status")
def status():
    refresh_account()
    wins  = bot_state["win_count"]
    total = bot_state["total_trades"]
    et    = get_et_time()
    return jsonify(clean_nan({
        "running": bot_state["running"],
        "killed": bot_state["killed"],
        "paper_mode": PAPER_MODE,
        "market_open": bot_state["market_open"],
        "in_trading_window": bot_state["in_trading_window"],
        "et_time": et.strftime("%H:%M ET"),
        "positions": bot_state["positions"],
        "closed_trades": bot_state["closed_trades"][-50:],
        "diary": bot_state["diary"][-100:],
        "day_pnl": bot_state["day_pnl"],
        "total_trades": total,
        "win_rate": round(wins/total*100) if total > 0 else 0,
        "long_stats": bot_state["long_stats"],
        "short_stats": bot_state["short_stats"],
        "signals": bot_state["signals"],
        "strategy": STRATEGY,
        "account_cash": bot_state["account_cash"],
        "account_equity": bot_state["account_equity"],
        "account_buying_power": bot_state["account_buying_power"],
        "market_regime": bot_state["market_regime"],
        "vix": bot_state["vix"],
        "vix_status": bot_state["vix_status"],
        "day_trades_used": bot_state["day_trades_used"],
        "short_lockouts": bot_state["short_lockouts"],
        "version": bot_state["version"]
    }))
 
@app.route("/diary")
def diary():
    return jsonify({"diary": bot_state["diary"]})
 
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
            if isinstance(t, str):
                ts = int(dt.fromisoformat(t.replace("Z","+00:00")).timestamp())
            else:
                ts = int(t)
            result.append({"time": ts, "open": b["open"], "high": b["high"],
                           "low": b["low"], "close": b["close"]})
        except:
            pass
    return jsonify(result)
 
@app.route("/history")
def history():
    return jsonify({"trades": bot_state["closed_trades"]})
 
@app.route("/")
def index():
    try:
        with open("index.html") as f:
            return f.read()
    except:
        return jsonify({
            "status": "ETF Bot v1.0 running",
            "symbols": SYMBOLS,
            "regime": bot_state["market_regime"],
            "vix": bot_state["vix"],
            "day_trades_used": bot_state["day_trades_used"]
        })
 
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
