import asyncio
import datetime as dt
from dataclasses import dataclass
from email.message import EmailMessage
import io
import json
import logging
import math
import os
import smtplib
import sys
import time
import random
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from ib_async import (
    IB,
    Contract,
    LimitOrder,
    Option,
    Stock,
    Ticker,
    util,
    wrapper,
)
from supabase import Client, create_client

# =====================================================================
# Monkey Patch for ib_async to prevent KeyError and hide cosmetic errors
# =====================================================================
original_contractDetails = wrapper.Wrapper.contractDetails
original_error = wrapper.Wrapper.error


def safe_contractDetails(self, reqId, contractDetails):
    # If the request was cleared from _results due to a timeout/disconnect, silently drop it.
    if hasattr(self, '_results') and reqId not in self._results:
        return
    original_contractDetails(self, reqId, contractDetails)


def safe_error(self, reqId, errorCode, errorString, advancedOrderRejectJson=''):
    # 10349: TIF set to DAY warning | 202: Order Canceled confirmation
    if errorCode in (10349, 202):
        return
    original_error(self, reqId, errorCode, errorString, advancedOrderRejectJson)


wrapper.Wrapper.contractDetails = safe_contractDetails
wrapper.Wrapper.error = safe_error

# Mute the noisy IBKR internal portfolio updates
logging.getLogger('ib_async').setLevel(logging.WARNING)

# =====================================================================
# 1. Logging Configuration (UTF-8 Enforced for Windows Emojis)
# =====================================================================
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

file_handler = logging.FileHandler("./livermore_strategy.log", encoding="utf-8")
stream_handler = logging.StreamHandler(sys.stdout)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, stream_handler],
)
logger = logging.getLogger("LivermoreTrader")


# =====================================================================
# Configuration & Credentials Loading
# =====================================================================
@dataclass
class StrategyConfig:
    ib_host: str = "127.0.0.1"
    ib_port: int = 7496
    ib_client_id: int = 101

    supabase_url: str = ""
    supabase_key: str = ""
    gmail_sender: str = ""
    gmail_password: str = ""
    gmail_receiver: str = ""

    max_strategy_allocation_pct: float = 0.25
    max_trade_allocation_pct: float = 0.05
    risk_per_trade_pct: float = 0.02
    min_cash_buffer_pct: float = 0.10
    max_open_trades: int = 3

    # Universe & Scanner Settings
    include_nasdaq_100: bool = True
    breakout_period_days: int = 252
    volume_surge_multiplier: float = 1.30
    trend_sma_fast: int = 50
    trend_sma_slow: int = 200
    consolidation_lookback: int = 20
    min_stock_price: float = 20.0
    min_avg_volume: int = 1_000_000
    scanner_batch_size: int = 50

    # New Optimized Breakout Parameters
    power_hour_minutes_open: float = 360.0  # 390 min total in a day, scan at 3:30PM = 360
    max_base_depth_pct: float = 0.20
    breakout_proximity_pct: float = 0.98

    target_dte_days: int = 365
    min_dte_days: int = 300
    max_dte_days: int = 450
    target_delta: float = 0.75
    max_spread_pct: float = 0.15

    min_delta_threshold: float = 0.50

    stop_loss_pct: float = 0.20
    trailing_stop_pct: float = 0.15
    target_profit_pct: float = 0.60

    chaser_interval_seconds: float = 3.0
    chaser_max_attempts: int = 8
    chaser_max_slippage_pct: float = 0.06
    chaser_step_pct: float = 0.005

    # New flag for debugging Phase 2 rejections
    debug_phase2_rejections: bool = True

    @classmethod
    def from_json(cls, filepath: str) -> "StrategyConfig":
        if not os.path.exists(filepath):
            logger.warning(f"Config {filepath} not found. Using defaults.")
            return cls()
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Helper for handling bool strings in json ("true" / "false")
        def parse_bool(val, default):
            if isinstance(val, bool): return val
            if isinstance(val, str): return val.lower() in ("true", "1", "yes")
            return default

        return cls(
            ib_host=data.get("IB_HOST", "127.0.0.1"),
            ib_port=int(data.get("IB_PORT", 7497)),
            ib_client_id=int(data.get("IB_CLIENT_ID", 101)),
            supabase_url=data.get("SUPABASE_URL", ""),
            supabase_key=data.get("SUPABASE_KEY", ""),
            gmail_sender=data.get("GMAIL_SENDER", ""),
            gmail_password=data.get("GMAIL_PASSWORD", ""),
            gmail_receiver=data.get("GMAIL_RECEIVER", ""),
            max_strategy_allocation_pct=float(data.get("MAX_STRAT_ALLOC", 0.25)),
            max_trade_allocation_pct=float(data.get("MAX_TRADE_ALLOC", 0.05)),
            risk_per_trade_pct=float(data.get("RISK_PER_TRADE", 0.01)),
            max_open_trades=int(data.get("MAX_OPEN_TRADES", 3)),

            # New Toggles & Limits
            include_nasdaq_100=parse_bool(data.get("INCLUDE_NASDAQ_100"), True),
            power_hour_minutes_open=float(data.get("POWER_HOUR_MINUTES_OPEN", 360.0)),
            max_base_depth_pct=float(data.get("MAX_BASE_DEPTH_PCT", 0.20)),
            breakout_proximity_pct=float(data.get("BREAKOUT_PROXIMITY_PCT", 0.98)),

            stop_loss_pct=float(data.get("STOP_LOSS_PCT", 0.20)),
            trailing_stop_pct=float(data.get("TRAILING_STOP_PCT", 0.15)),
            target_profit_pct=float(data.get("TARGET_PROFIT_PCT", 0.60)),
            min_delta_threshold=float(data.get("MIN_DELTA_THRESHOLD", 0.50)),

            # Now properly parsing chaser settings from JSON
            chaser_interval_seconds=float(data.get("CHASER_INTERVAL_SECONDS", 3.0)),
            chaser_max_attempts=int(data.get("CHASER_MAX_ATTEMPTS", 8)),
            chaser_max_slippage_pct=float(data.get("CHASER_MAX_SLIPPAGE_PCT", 0.06)),
            chaser_step_pct=float(data.get("CHASER_STEP_PCT", 0.005)),

            # Debug flag
            debug_phase2_rejections=parse_bool(data.get("DEBUG_PHASE2_REJECTIONS"), False),
        )


# =====================================================================
# Market Calendar & Sleep Engine
# =====================================================================
class MarketSchedule:
    TIMEZONE = ZoneInfo("America/New_York")

    @classmethod
    def get_ny_now(cls) -> dt.datetime:
        return dt.datetime.now(cls.TIMEZONE)

    @classmethod
    def is_holiday(cls, date: dt.date) -> bool:
        year = date.year
        holidays = {
            dt.date(year, 1, 1), dt.date(year, 6, 19),
            dt.date(year, 7, 4), dt.date(year, 12, 25),
        }
        holidays.add(cls._get_nth_weekday(year, 1, 0, 3))
        holidays.add(cls._get_nth_weekday(year, 2, 0, 3))
        holidays.add(cls._get_last_weekday(year, 5, 0))
        holidays.add(cls._get_nth_weekday(year, 9, 0, 1))
        holidays.add(cls._get_nth_weekday(year, 11, 3, 4))

        observed_holidays = set()
        for h in holidays:
            if h.weekday() == 5:
                observed_holidays.add(h - dt.timedelta(days=1))
            elif h.weekday() == 6:
                observed_holidays.add(h + dt.timedelta(days=1))
            else:
                observed_holidays.add(h)
        return date in observed_holidays

    @staticmethod
    def _get_nth_weekday(year: int, month: int, weekday: int, n: int) -> dt.date:
        count = 0
        for day in range(1, 32):
            try:
                d = dt.date(year, month, day)
                if d.weekday() == weekday:
                    count += 1
                    if count == n: return d
            except ValueError:
                break
        raise ValueError("Invalid date")

    @staticmethod
    def _get_last_weekday(year: int, month: int, weekday: int) -> dt.date:
        for day in range(31, 0, -1):
            try:
                d = dt.date(year, month, day)
                if d.weekday() == weekday: return d
            except ValueError:
                continue
        raise ValueError("Invalid date")

    @classmethod
    def is_market_open(cls) -> bool:
        now = cls.get_ny_now()
        if now.weekday() >= 5 or cls.is_holiday(now.date()): return False
        return dt.time(9, 30) <= now.time() < dt.time(16, 0)

    @classmethod
    def get_seconds_until_next_open(cls) -> float:
        now = cls.get_ny_now()
        target_date = now.date()
        if now.time() >= dt.time(16, 0) or now.weekday() >= 5 or cls.is_holiday(target_date):
            target_date += dt.timedelta(days=1)
        while target_date.weekday() >= 5 or cls.is_holiday(target_date):
            target_date += dt.timedelta(days=1)
        next_open = dt.datetime.combine(target_date, dt.time(9, 30, 0), tzinfo=cls.TIMEZONE)
        return max((next_open - now).total_seconds(), 60.0)


# =====================================================================
# Non-Blocking Gmail Notification Manager
# =====================================================================
class EmailNotifier:
    def __init__(self, config: StrategyConfig):
        self.config = config

    def _sync_send(self, subject: str, body: str):
        if not self.config.gmail_sender: return
        try:
            msg = EmailMessage()
            msg.set_content(body)
            msg["Subject"] = f"[ALGO] {subject}"
            msg["From"] = self.config.gmail_sender
            msg["To"] = self.config.gmail_receiver
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as server:
                server.login(self.config.gmail_sender, self.config.gmail_password)
                server.send_message(msg)
        except Exception as e:
            logger.error("Email failed: %s", e)

    async def send_alert_async(self, subject: str, body: str):
        await asyncio.to_thread(self._sync_send, subject, body)


# =====================================================================
# Supabase Database Manager
# =====================================================================
class SupabaseManager:
    def __init__(self, config: StrategyConfig):
        self.client = create_client(config.supabase_url, config.supabase_key) if config.supabase_url else None

    async def get_open_trades(self) -> List[Dict]:
        if not self.client: return []

        def _q():
            return self.client.table("livermore_trades").select("*").eq("status", "OPEN").execute()

        try:
            res = await asyncio.to_thread(_q)
            return res.data or []
        except Exception as e:
            # Catch timeouts and pydantic parsing errors smoothly
            logger.warning(f"⚠️ Supabase GET open_trades error (likely timeout): {e}")
            return []

    async def record_entry(self, data: Dict):
        if not self.client: return None

        def _i():
            return self.client.table("livermore_trades").insert(data).execute()

        try:
            await asyncio.to_thread(_i)
        except Exception as e:
            logger.error(f"⚠️ Supabase INSERT error: {e}")

    async def update_trade_high(self, trade_id: str, high: float):
        if not self.client: return

        def _u():
            return self.client.table("livermore_trades").update({"underlying_highest_price": high}).eq("trade_id",
                                                                                                       trade_id).execute()

        try:
            await asyncio.to_thread(_u)
        except Exception as e:
            logger.warning(f"⚠️ Supabase UPDATE HIGH error: {e}")

    async def record_exit(self, trade_id: str, exit_price: float, reason: str, pnl: float):
        if not self.client: return
        payload = {"exit_price": exit_price, "status": "CLOSED", "exit_reason": reason, "realized_pnl": pnl}

        def _u():
            return self.client.table("livermore_trades").update(payload).eq("trade_id", trade_id).execute()

        try:
            await asyncio.to_thread(_u)
        except Exception as e:
            logger.error(f"⚠️ Supabase EXIT error (Verify manually if trade closed at broker): {e}")


# =====================================================================
# Scanner (Robust YFinance Multi-Index Parsing)
# =====================================================================
class LivermoreScanner:
    def __init__(self, config: StrategyConfig):
        self.config = config

    def _sync_fetch_tickers(self) -> List[str]:
        try:
            # Full browser User-Agent to prevent anti-bot blocking
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
            }

            # Always fetch S&P 500
            sp500_res = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=headers)
            tickers = pd.read_html(io.StringIO(sp500_res.text))[0]["Symbol"].tolist()
            logger.info(f"✅ Successfully fetched {len(tickers)} S&P 500 tickers.")

            # Conditionally fetch Nasdaq 100 with robust fallbacks
            if self.config.include_nasdaq_100:
                ndx_urls = [
                    "https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies",
                    "https://en.wikipedia.org/wiki/Nasdaq-100",
                    "https://www.slickcharts.com/nasdaq100"
                ]

                ndx_success = False
                for url in ndx_urls:
                    if ndx_success: break
                    try:
                        ndx_res = requests.get(url, headers=headers, timeout=10)
                        tables = pd.read_html(io.StringIO(ndx_res.text))

                        for tbl in tables:
                            # Standardize column headers to catch variations
                            col_names = [str(c).lower() for c in tbl.columns]

                            ticker_col = None
                            for orig_col, lower_col in zip(tbl.columns, col_names):
                                if "ticker" in lower_col or "symbol" in lower_col:
                                    ticker_col = orig_col
                                    break

                            # Ensure the table is large enough to be an index list
                            if ticker_col and len(tbl) > 50:
                                ndx_tickers = tbl[ticker_col].tolist()
                                # Clean up whitespace to prevent yfinance errors
                                ndx_tickers = [str(t).strip() for t in ndx_tickers if pd.notna(t)]
                                tickers.extend(ndx_tickers)

                                source_name = "Slickcharts" if "slickcharts" in url else "Wikipedia"
                                logger.info(
                                    f"✅ Successfully fetched {len(ndx_tickers)} Nasdaq 100 tickers from {source_name}.")
                                ndx_success = True
                                break
                    except Exception:
                        # Silently pass to the next fallback URL if this one fails
                        pass

                if not ndx_success:
                    logger.warning("Could not fetch Nasdaq-100 constituents from any source.")

            # Remove duplicates and format for yfinance (e.g., BRK.B to BRK-B)
            combined = list(set(tickers))
            logger.info(f"📊 Total unique tickers merged for Phase 1 scan: {len(combined)}")

            return [t.replace(".", "-") for t in combined]

        except Exception as e:
            logger.error(f"Failed to fetch market universe: {e}")
            return ["AAPL", "MSFT", "NVDA", "AMZN", "META"]

    async def get_sp500_tickers(self) -> List[str]:
        return await asyncio.to_thread(self._sync_fetch_tickers)

    def _sync_download(self, tickers: List[str]) -> pd.DataFrame:
        try:
            return yf.download(tickers, period="15mo", interval="1d", auto_adjust=True, progress=False, threads=False)
        except:
            return pd.DataFrame()

    def _extract_ticker_df(self, data: pd.DataFrame, ticker: str, is_single: bool) -> pd.DataFrame:
        try:
            if is_single:
                return pd.DataFrame({"Close": data["Close"], "Volume": data["Volume"]}).dropna()
            if "Close" in data.columns.levels[0]:
                return pd.DataFrame({"Close": data["Close"][ticker], "Volume": data["Volume"][ticker]}).dropna()
            else:
                return data[ticker][["Close", "Volume"]].dropna()
        except:
            return pd.DataFrame()

    async def build_premarket_watchlist(self, tickers: List[str]) -> List[str]:
        watchlist = []
        for i in range(0, len(tickers), self.config.scanner_batch_size):
            batch = tickers[i: i + self.config.scanner_batch_size]
            data = await asyncio.to_thread(self._sync_download, batch)

            # Add a 1-second delay to prevent Yahoo Finance IP bans
            await asyncio.sleep(1.0)

            if data.empty: continue

            for t in batch:
                df = self._extract_ticker_df(data, t, len(batch) == 1)
                if len(df) < self.config.breakout_period_days: continue

                close = float(df["Close"].iloc[-1])
                if close < self.config.min_stock_price: continue

                sma_50 = float(df["Close"].rolling(self.config.trend_sma_fast).mean().iloc[-1])
                sma_200 = float(df["Close"].rolling(self.config.trend_sma_slow).mean().iloc[-1])
                high_52w = float(df["Close"].iloc[-self.config.breakout_period_days:-1].max())

                if close > sma_50 > sma_200 and close >= (high_52w * 0.90):
                    watchlist.append(t)

        with open(f"watchlist_{MarketSchedule.get_ny_now().date().isoformat()}.json", "w") as f:
            json.dump(watchlist, f)
        return watchlist

    async def scan_power_hour_triggers(self, watchlist: List[str]) -> Tuple[List[str], Dict[str, List[str]]]:
        if not watchlist: return [], {}
        qualifying = []
        rejections = {
            "No Data": [], "Low Avg Volume": [], "No Breakout": [],
            "Base Too Deep": [], "No Volume Surge": []
        }

        data = await asyncio.to_thread(self._sync_download, watchlist)
        if data.empty:
            rejections["No Data"] = watchlist
            return [], rejections

        for t in watchlist:
            df = self._extract_ticker_df(data, t, len(watchlist) == 1)
            if df.empty:
                rejections["No Data"].append(t)
                continue

            close, vol = float(df["Close"].iloc[-1]), float(df["Volume"].iloc[-1])
            vol_sma = float(df["Volume"].rolling(20).mean().iloc[-1])

            if vol_sma < self.config.min_avg_volume:
                rejections["Low Avg Volume"].append(t)
                continue

            high_52w = float(df["Close"].iloc[-self.config.breakout_period_days:-1].max())
            base_min = float(df["Close"].iloc[-self.config.consolidation_lookback:-1].min())

            # Local high based on the base lookback period
            local_high = float(df["Close"].iloc[-self.config.consolidation_lookback - 1:-1].max())

            # Project current volume to full day
            projected_daily_vol = vol * (390.0 / self.config.power_hour_minutes_open)

            if close < local_high or close < (high_52w * self.config.breakout_proximity_pct):
                rejections["No Breakout"].append(t)
                continue
            if ((high_52w - base_min) / high_52w) > self.config.max_base_depth_pct:
                rejections["Base Too Deep"].append(t)
                continue
            if (projected_daily_vol / vol_sma) < self.config.volume_surge_multiplier:
                rejections["No Volume Surge"].append(t)
                continue

            qualifying.append(t)

        return qualifying, {k: v for k, v in rejections.items() if v}


# =====================================================================
# Adaptive Limit Order Chaser Engine
# =====================================================================
class LimitOrderChaser:
    def __init__(self, ib: IB, config: StrategyConfig):
        self.ib = ib
        self.config = config

    async def get_market_quote(self, contract: Contract) -> Tuple[float, float, float]:
        try:
            # Wrap the IBKR request in a strict timeout
            tickers = await asyncio.wait_for(self.ib.reqTickersAsync(contract), timeout=2.0)
        except (asyncio.TimeoutError, TimeoutError):
            t = self.ib.ticker(contract)
            if not t:
                return 0.0, 0.0, 0.0
            tickers = [t]

        if not tickers: return 0.0, 0.0, 0.0

        t = tickers[0]
        bid = t.bid if t.bid and t.bid > 0 else 0.0
        ask = t.ask if t.ask and t.ask > 0 else 0.0

        last_raw = t.last if t.last and t.last > 0 else (t.close or 0.0)
        last = 0.0 if math.isnan(last_raw) else last_raw

        if t.modelGreeks and t.modelGreeks.optPrice and t.modelGreeks.optPrice > 0:
            model_price = t.modelGreeks.optPrice
            if bid > 0 and ask > 0:
                if bid <= model_price <= ask:
                    initial_mid = model_price
                else:
                    mid = (bid + ask) / 2
                    initial_mid = min(mid, model_price * 1.05)
            else:
                initial_mid = model_price
        else:
            initial_mid = ((bid + ask) / 2) if bid > 0 and ask > 0 else last

        return bid, ask, initial_mid

    async def execute_chasing_limit_order(self, contract: Contract, action: str, qty: int) -> Tuple[bool, float, int]:
        bid, ask, initial_mid = await self.get_market_quote(contract)
        if initial_mid <= 0: return False, 0.0, 0

        def round_to_05(val: float) -> float:
            return round(val * 20) / 20.0

        is_buy = action.upper() == "BUY"

        current_limit = round_to_05(initial_mid)
        raw_max_limit = initial_mid * (
            1.0 + self.config.chaser_max_slippage_pct if is_buy else 1.0 - self.config.chaser_max_slippage_pct)
        max_limit = round_to_05(raw_max_limit)

        order = LimitOrder(action, qty, current_limit)
        trade = self.ib.placeOrder(contract, order)

        for attempt in range(1, self.config.chaser_max_attempts + 1):
            await asyncio.sleep(self.config.chaser_interval_seconds)

            if trade.isDone():
                break  # Exit loop immediately if filled or canceled early

            raw_next_limit = current_limit * (1.0 + self.config.chaser_step_pct) if is_buy else current_limit * (
                    1.0 - self.config.chaser_step_pct)

            if is_buy:
                current_limit = round_to_05(min(raw_next_limit, max_limit))
            else:
                current_limit = round_to_05(max(raw_next_limit, max_limit))

            order.lmtPrice = current_limit
            self.ib.placeOrder(contract, order)

        # If the trade isn't marked "Done" yet, we cancel the remainder
        if not trade.isDone():
            self.ib.cancelOrder(order)
            wait_time = 0.0
            # Wait up to 5 seconds for IBKR to confirm the cancellation
            while not trade.isDone() and wait_time < 5.0:
                await asyncio.sleep(0.5)
                wait_time += 0.5

        # --- FIX: The 1-second buffer to catch late fills ---
        await asyncio.sleep(1.0)

        # Calculate filled quantity by summing actual execution reports (trade.fills)
        exec_filled = sum(f.execution.shares for f in trade.fills)
        status_filled = trade.orderStatus.filled if trade.orderStatus.filled else 0

        qty_filled = int(max(exec_filled, status_filled))

        if qty_filled > 0:
            if exec_filled > 0:
                # Calculate exact average fill price from the execution reports
                total_cost = sum(f.execution.shares * f.execution.price for f in trade.fills)
                final_price = total_cost / exec_filled
            else:
                final_price = trade.orderStatus.avgFillPrice or current_limit

            return True, final_price, qty_filled

        return False, 0.0, 0


# =====================================================================
# Main Trading Engine
# =====================================================================
class LivermoreTradingEngine:
    def __init__(self, config: StrategyConfig):
        self.config = config
        self.ib = IB()
        self.db = SupabaseManager(config)
        self.scanner = LivermoreScanner(config)
        self.notifier = EmailNotifier(config)
        self.chaser = LimitOrderChaser(self.ib, config)

    async def connect(self):
        if not self.ib.isConnected():
            await self.ib.connectAsync(self.config.ib_host, self.config.ib_port, self.config.ib_client_id)
            self.ib.reqMarketDataType(4)

    async def sync_positions_with_broker(self):
        logger.info("🔄 Syncing Supabase state with live IBKR positions...")
        positions = await self.ib.reqPositionsAsync()
        ib_holdings = {p.contract.conId: p.position for p in positions if p.position != 0}

        db_trades = await self.db.get_open_trades()

        # Pre-fetch today's executions in case we need to reconcile manual sells
        executions = []
        if db_trades:
            try:
                executions = await self.ib.reqExecutionsAsync()
            except Exception as e:
                logger.warning(f"Could not fetch executions for reconciliation: {e}")

        for t in db_trades:
            con_id = t["con_id"]
            if ib_holdings.get(con_id, 0) == 0:
                logger.warning(
                    f"⚠️ Phantom DB Position: {t['symbol']} (conId {con_id}) is marked OPEN in Supabase but flat at IBKR. Reconciling..."
                )

                # Default to breakeven if manual exit isn't found in today's session
                exit_price = float(t["option_entry_price"])
                pnl = 0.0
                reason = "SYNC_RECONCILIATION (UNKNOWN PRICE)"

                # Check today's executions for the manual sell
                for fill in executions:
                    if fill.contract.conId == con_id and fill.execution.side == "SLD":
                        exit_price = fill.execution.price
                        pnl = (exit_price - float(t["option_entry_price"])) * 100 * int(t["contracts"])
                        reason = "MANUAL_CLOSE_RECONCILED"
                        break

                await self.db.record_exit(t["trade_id"], exit_price, reason, pnl)

    async def get_account_summary(self) -> Dict[str, float]:
        vals = await self.ib.accountSummaryAsync()
        return {
            "net_liq": next((float(v.value) for v in vals if v.tag == "NetLiquidation"), 0.0),
            "available_funds": next((float(v.value) for v in vals if v.tag == "AvailableFunds"), 0.0)
        }

    async def print_holdings_status(self):
        trades = await self.db.get_open_trades()
        if not trades:
            logger.info("💼 [PORTFOLIO] No open positions currently.")
            return

        lines = ["\n" + "=" * 60, "💼 ACTIVE LIVERMORE HOLDINGS", "=" * 60]
        total_pnl = 0.0

        for t in trades:
            contract = Contract(conId=t["con_id"])
            await self.ib.qualifyContractsAsync(contract)
            _, _, curr = await self.chaser.get_market_quote(contract)

            # Fixed: Use option_entry_price
            entry = float(t["option_entry_price"])
            qty = int(t["contracts"])
            if curr > 0:
                unrealized = (curr - entry) * 100 * qty
                pct = ((curr - entry) / entry) * 100
            else:
                unrealized, pct = 0.0, 0.0

            total_pnl += unrealized
            marker = "🟢" if unrealized >= 0 else "🔴"
            lines.append(f"{marker} {t['symbol']:<5} | {t['option_symbol']} | Qty: {qty}")
            lines.append(f"   Entry: ${entry:.2f} | Mark: ${curr:.2f} | PnL: ${unrealized:+.2f} ({pct:+.2f}%)")
            lines.append("-" * 60)

        lines.append(f"💰 Total Unrealized Strategy PnL: ${total_pnl:+.2f}")
        lines.append("=" * 60 + "\n")
        logger.info("\n".join(lines))

    async def select_best_leap_contract(self, symbol: str) -> Tuple[Optional[Contract], str]:
        stock = Stock(symbol, "SMART", "USD")
        await self.ib.qualifyContractsAsync(stock)
        chains = await self.ib.reqSecDefOptParamsAsync(stock.symbol, "", stock.secType, stock.conId)

        if not chains:
            return None, "NO_OPTION_CHAINS_AVAILABLE"

        chain = next((c for c in chains if c.exchange == "SMART"), chains[0])
        today = dt.date.today()

        valid_exp = [e for e in chain.expirations if self.config.min_dte_days <= (
                dt.datetime.strptime(e, "%Y%m%d").date() - today).days <= self.config.max_dte_days]

        if not valid_exp:
            return None, f"NO_EXPIRATIONS_IN_RANGE ({self.config.min_dte_days}-{self.config.max_dte_days} days)"

        chosen_exp = sorted(valid_exp, key=lambda x: abs(
            (dt.datetime.strptime(x, "%Y%m%d").date() - today).days - self.config.target_dte_days))[0]

        _, _, stock_price = await self.chaser.get_market_quote(stock)
        if stock_price <= 0:
            return None, "INVALID_UNDERLYING_STOCK_PRICE"

        approx_strike = stock_price * 0.88
        query_contract = Option(symbol=symbol, lastTradeDateOrContractMonth=chosen_exp, right="C", exchange="SMART",
                                currency="USD")

        try:
            details = await self.ib.reqContractDetailsAsync(query_contract)
            if not details:
                query_contract.exchange = ""
                details = await self.ib.reqContractDetailsAsync(query_contract)
        except Exception as e:
            logger.warning("Error fetching contract details for %s: %s", symbol, e)
            return None, f"CONTRACT_DETAILS_ERROR: {e}"

        if not details:
            return None, "NO_CONTRACT_DETAILS_FOUND"

        valid_contracts = sorted([d.contract for d in details if d.contract.strike > 0],
                                 key=lambda c: abs(c.strike - approx_strike))
        candidate_contracts = valid_contracts[:4]

        if not candidate_contracts:
            return None, "NO_VALID_STRIKES_NEAR_TARGET"

        # --- NEW RETRY LOGIC FOR WIDE SPREADS ---
        for attempt in range(1, 4):
            tickers = await self.ib.reqTickersAsync(*candidate_contracts)
            best_contract = None
            closest_delta_diff = float("inf")
            rejection_reasons = []

            for t in tickers:
                bid, ask = t.bid or 0.0, t.ask or 0.0
                if bid <= 0 or ask <= 0:
                    rejection_reasons.append(f"Strike {t.contract.strike}: Invalid Quote (Bid: {bid}, Ask: {ask})")
                    continue

                spread_pct = (ask - bid) / bid
                if spread_pct > self.config.max_spread_pct:
                    rejection_reasons.append(
                        f"Strike {t.contract.strike}: Spread {spread_pct:.1%} > {self.config.max_spread_pct:.1%}")
                    continue

                delta = t.modelGreeks.delta if (t.modelGreeks and t.modelGreeks.delta) else None
                if delta is not None:
                    diff = abs(delta - self.config.target_delta)
                    if diff < closest_delta_diff:
                        closest_delta_diff = diff
                        best_contract = t.contract
                else:
                    return t.contract, "SUCCESS_NO_GREEKS"

            if best_contract:
                return best_contract, "SUCCESS"

            # If we failed to find a valid contract, log the exact reasons and wait 10 seconds before checking quotes again
            logger.info(
                f"⏳ [{symbol}] Attempt {attempt}/3 rejected. Reasons: {', '.join(rejection_reasons)}. Retrying in 10s...")
            await asyncio.sleep(10.0)

        return None, f"FAILED_ALL_ATTEMPTS. Final reasons: {', '.join(rejection_reasons)}"

    async def execute_entry(self, symbol: str, net_liq: float, funds: float, strat_alloc: float):
        # 1. Unpack the tuple correctly!
        leap, rejection_reason = await self.select_best_leap_contract(symbol)

        if not leap:
            logger.info(f"🛑 [{symbol}] Options Rejected. Reason: {rejection_reason}")
            return

        _, _, mid = await self.chaser.get_market_quote(leap)
        if mid <= 0:
            logger.info(f"🛑 [{symbol}] LEAP found, but could not get a valid mid-price quote.")
            return

        # Fetch underlying stock quote to anchor stops and targets
        stock = Stock(symbol, "SMART", "USD")
        await self.ib.qualifyContractsAsync(stock)
        _, _, stock_price = await self.chaser.get_market_quote(stock)

        if stock_price <= 0:
            logger.info(f"🛑 [{symbol}] Could not get a valid quote for the underlying stock. Aborting entry.")
            return

        # --- NEW BUDGET MATH ---
        # 1. Determine total capital allocated to this strategy
        strategy_budget = net_liq * self.config.max_strategy_allocation_pct

        # 2. Cap the individual trade relative to that strategy budget
        trade_budget_limit = strategy_budget * self.config.max_trade_allocation_pct

        risk_budget = net_liq * self.config.risk_per_trade_pct
        risk_qty = int(risk_budget // (mid * 100 * self.config.stop_loss_pct)) if self.config.stop_loss_pct > 0 else 0

        # 3. Apply the updated limit
        cap_limit = min(trade_budget_limit,
                        max(0, funds - (net_liq * self.config.min_cash_buffer_pct)))

        cap_qty = int(cap_limit // (mid * 100))
        qty = max(0, min(risk_qty, cap_qty))
        # -----------------------

        if qty < 1:
            logger.info(f"🛑 [{symbol}] LEAP found, but not enough capital to buy 1 contract safely.")
            return

        success, fill, filled_qty = await self.chaser.execute_chasing_limit_order(leap, "BUY", qty)
        if success and filled_qty > 0:
            total_cap = filled_qty * fill * 100

            stop = round(stock_price * (1.0 - self.config.stop_loss_pct), 2)
            target = round(stock_price * (1.0 + self.config.target_profit_pct), 2)

            await self.db.record_entry({
                "symbol": symbol, "con_id": leap.conId, "option_symbol": leap.localSymbol,
                "strike": float(leap.strike), "expiration": leap.lastTradeDateOrContractMonth,
                "right_type": leap.right,
                "option_entry_price": fill,
                "contracts": filled_qty,
                "capital_allocated": total_cap,
                "underlying_entry_price": stock_price,
                "underlying_stop_loss_price": stop,
                "underlying_target_price": target,
                "underlying_highest_price": stock_price,
                "status": "OPEN"
            })
            await self.notifier.send_alert_async(
                f"BUY EXECUTED: {symbol}",
                f"Bought {filled_qty} of {qty} requested {leap.localSymbol} @ ${fill:.2f}\n"
                f"Underlying Anchor: ${stock_price:.2f} | Stop Loss: ${stop:.2f}"
            )
        else:
            logger.info(f"🛑 [{symbol}] Attempted to buy LEAP, but the limit order chaser timed out with 0 fills.")

    async def monitor_and_exit_positions(self):
        trades = await self.db.get_open_trades()
        for t in trades:
            symbol = t["symbol"]
            con = Contract(conId=t["con_id"])
            stock = Stock(symbol, "SMART", "USD")

            await self.ib.qualifyContractsAsync(con)
            await self.ib.qualifyContractsAsync(stock)

            # Fixed: Get quote for the underlying stock
            _, _, stock_curr = await self.chaser.get_market_quote(stock)
            if stock_curr <= 0: continue

            # Get quote and Greeks for the option (needed for delta check and selling)
            _, _, opt_curr = await self.chaser.get_market_quote(con)
            if opt_curr <= 0: continue

            ticker = self.ib.ticker(con)
            live_delta = None
            if ticker and ticker.modelGreeks and ticker.modelGreeks.delta:
                live_delta = ticker.modelGreeks.delta

            # Fixed: Update highest underlying price for trailing stop logic
            high = max(float(t["underlying_highest_price"]), stock_curr)
            if stock_curr > float(t["underlying_highest_price"]):
                await self.db.update_trade_high(t["trade_id"], stock_curr)

            reason = None

            # Fixed: Evaluate exits based purely on the UNDERLYING stock price action
            if stock_curr <= float(t["underlying_stop_loss_price"]):
                reason = "STOP_LOSS"
            elif stock_curr <= high * (1.0 - self.config.trailing_stop_pct) and stock_curr > float(
                    t["underlying_entry_price"]):
                reason = "TRAILING_STOP"
            elif stock_curr >= float(t["underlying_target_price"]):
                reason = "TARGET_PROFIT"
            elif live_delta is not None and live_delta < self.config.min_delta_threshold:
                reason = f"DELTA_DECAY_{live_delta:.2f}"

            if reason:
                # Trigger the limit chaser on the OPTION contract
                success, fill, qty = await self.chaser.execute_chasing_limit_order(con, "SELL", int(t["contracts"]))
                if success:
                    pnl = (fill - float(t["option_entry_price"])) * 100 * qty
                    await self.db.record_exit(t["trade_id"], fill, reason, pnl)
                    await self.notifier.send_alert_async(
                        f"SELL EXECUTED: {symbol}",
                        f"Reason: {reason} (Stock crossed threshold at ${stock_curr:.2f})\n"
                        f"Sold Option @ ${fill:.2f} | PnL: ${pnl:.2f}"
                    )


# =====================================================================
# Main Execution Loop
# =====================================================================
async def main(client_id_override: int = None):
    # Mute the noisy IBKR internal portfolio updates
    logging.getLogger('ib_async').setLevel(logging.WARNING)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_config_path = os.path.join(script_dir, "livermore.json")

    config = StrategyConfig.from_json(local_config_path)
    if client_id_override:
        config.ib_client_id = client_id_override

    engine = LivermoreTradingEngine(config)
    scanned_today = False
    loop_counter = 0
    is_initial_connect = True

    try:
        while True:
            try:
                ny_now = MarketSchedule.get_ny_now()
                is_valid_trading_day = (ny_now.weekday() < 5) and not MarketSchedule.is_holiday(ny_now.date())

                if not engine.ib.isConnected():
                    if is_initial_connect:
                        await engine.connect()
                        await engine.sync_positions_with_broker()
                        await engine.print_holdings_status()
                        loop_counter = 0
                        is_initial_connect = False
                    else:
                        logger.error("🔌 TWS disconnected mid-session. Bubbling to watchdog for a fresh session...")
                        raise ConnectionError("Disconnected from TWS. Forcing clean restart.")

                if not MarketSchedule.is_market_open():
                    scanned_today = False
                    sleep = MarketSchedule.get_seconds_until_next_open()
                    logger.info("🌙 Market closed. Sleeping %.1f hours.", sleep / 3600)
                    await asyncio.sleep(sleep)
                    continue

                if is_valid_trading_day:
                    today_str = ny_now.date().isoformat()
                    cache_file = f"watchlist_{today_str}.json"
                    if not os.path.exists(cache_file):
                        raw = await engine.scanner.get_sp500_tickers()
                        watchlist = await engine.scanner.build_premarket_watchlist(raw)
                    else:
                        try:
                            with open(cache_file, "r") as f:
                                watchlist = json.load(f)
                        except:
                            raw = await engine.scanner.get_sp500_tickers()
                            watchlist = await engine.scanner.build_premarket_watchlist(raw)

                await engine.monitor_and_exit_positions()

                if dt.time(15, 30) <= ny_now.time() <= dt.time(15, 55) and not scanned_today:
                    logger.info("🎯 [POWER HOUR] Trigger scan starting...")
                    acc = await engine.get_account_summary()
                    trades = await engine.db.get_open_trades()

                    open_count = len(trades)
                    slots_available = config.max_open_trades - open_count
                    alloc = sum(float(t["capital_allocated"]) for t in trades)
                    max_alloc = acc.get("net_liq", 0) * config.max_strategy_allocation_pct

                    if slots_available <= 0:
                        logger.info(
                            f"🛑 [PHASE 2 ABORTED] Max open trades reached ({open_count}/{config.max_open_trades}).")
                    elif alloc >= max_alloc:
                        logger.info(
                            f"🛑 [PHASE 2 ABORTED] Strategy capital allocation reached (${alloc:.2f}/${max_alloc:.2f}).")
                    else:
                        candidates, rejections = await engine.scanner.scan_power_hour_triggers(watchlist)

                        if config.debug_phase2_rejections:
                            logger.info(f"🔍 [DEBUG] Phase 2 Rejections Breakdown:")
                            for reason, tickers in rejections.items():
                                if tickers:
                                    # Print up to 5 examples to keep logs clean
                                    examples = ", ".join(tickers[:5])
                                    suffix = "..." if len(tickers) > 5 else ""
                                    logger.info(f"   -> {reason}: {len(tickers)} tickers (e.g. {examples}{suffix})")

                        active_syms = {t["symbol"] for t in trades}
                        new = [c for c in candidates if c not in active_syms][:slots_available]

                        if new:
                            tasks = [
                                engine.execute_entry(sym, acc.get("net_liq", 0), acc.get("available_funds", 0), alloc)
                                for sym in new]

                            # FIX: Use return_exceptions=True so a crash in one stock doesn't kill the others
                            results = await asyncio.gather(*tasks, return_exceptions=True)

                            # Log any exceptions that happened to specific stocks cleanly
                            for sym, res in zip(new, results):
                                if isinstance(res, Exception):
                                    logger.error(f"🛑 [{sym}] Uncaught error during entry execution: {res}")
                        else:
                            logger.info("🛑 Scan complete: Didn't find any opportunities.")

                    scanned_today = True

                # Print Holding Status every 5 minutes (loop runs approx every 60s)
                if loop_counter % 5 == 0:
                    await engine.print_holdings_status()

                loop_counter += 1
                await asyncio.sleep(60)

            except (ConnectionError, ConnectionRefusedError, TimeoutError, OSError):
                logger.error("🚨 Socket Disconnected. Bubbling to watchdog...")
                raise
            except Exception as e:
                logger.error("Loop error: %s", e, exc_info=True)
                await asyncio.sleep(30)

    finally:
        # Cleanly sever the IB connection before allowing the async loop to exit
        if engine.ib.isConnected():
            logger.info("🔌 Severing IBKR connection cleanly...")
            engine.ib.disconnect()


if __name__ == "__main__":
    while True:
        loop = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            util.run(main(client_id_override=random.randint(1000, 9999)))
        except (ConnectionError, ConnectionRefusedError, TimeoutError, OSError):
            logger.error("⚠️ Reconnecting in 60s...")
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("🛑 Keyboard interrupt received. Exiting.")
            break
        except Exception as e:
            logger.error("🚨 Fatal Error: %s", e, exc_info=True)
            time.sleep(60)
        finally:
            if loop and not loop.is_closed():
                try:
                    loop.run_until_complete(loop.shutdown_asyncgens())
                    loop.close()
                except Exception:
                    pass