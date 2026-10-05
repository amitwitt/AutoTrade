import asyncio
import logging
import time
import io
import math
import smtplib
import concurrent.futures
import sys
import os
import uuid
import random
from email.mime.text import MIMEText
from datetime import datetime, time as dt_time
import pytz
import pandas as pd
import yfinance as yf
import requests
import aiohttp
from supabase import create_client, Client
from ib_async import IB, Stock, Option, LimitOrder, Contract, util

# ==============================================================================
# LOGGING & CONSOLE NOISE FILTERING
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(message)s')

for logger_name in ["ib_async", "ib_async.wrapper", "ib_async.client", "ib_async.ib", "asyncio"]:
    log = logging.getLogger(logger_name)
    log.setLevel(logging.CRITICAL)
    log.propagate = False


def handle_ib_error(reqId, errorCode, errorString, contract):
    ignored_codes = {2104, 2106, 2158, 10091, 10167, 162, 300, 10197, 10349}
    if errorCode in ignored_codes:
        return
    print(f"⚠️ IBKR Notice [{errorCode}] (reqId {reqId}): {errorString}")


# ==============================================================================
# CONFIGURATION & ACTIVE CHASER PARAMETERS
# ==============================================================================
CONFIG = {
    "SCAN_NASDAQ_100": True,
    "MAX_OPEN_POSITIONS": 5,
    "MAX_STRATEGY_ALLOCATION": 0.5,
    "MAX_TRADE_ALLOCATION": 0.2,
    "LEAPS_DTE_TARGET": 365,
    "LEAPS_DELTA_TARGET": 0.80,

    # --- FILTERING PARAMETERS ---
    "ENABLE_SECTOR_FILTER": True,  # True = Strictly drop non-target sectors. False = Keep them but cap them.
    "MAX_NON_TARGET_SECTOR_PCT": 0.20,  # Max 20% of MAX_OPEN_POSITIONS can be non-target sectors
    "MAX_ZACKS_RANK": 2,
    "REQUIRE_200_SMA_UPTREND": True,  # Toggle 200-day secular bull market check

    # --- PROFIT & LOSS MANAGEMENT ---
    "TAKE_PROFIT_PCT": 0.50,
    "TRAILING_STOP_PCT": 0.10,
    "STOP_LOSS_TYPE": "UNDERLYING",  # Toggle: "UNDERLYING" or "LEAP"
    "STOP_LOSS_PCT": 0.25,  # Trigger if STOP_LOSS_TYPE = "LEAP"
    "UNDERLYING_STOP_LOSS_PCT": 0.10,  # Trigger if STOP_LOSS_TYPE = "UNDERLYING"

    # --- EXECUTION SAFETY LIMITS ---
    "MAX_BUY_SLIPPAGE_PCT": 0.03,
    "MAX_SELL_SLIPPAGE_PCT": 0.04,
    "MAX_LEAP_SPREAD_PCT": 0.20,

    "MAX_UNEXPECTED_DROPS": 1,  # Max positions that can disappear from IBKR at once before rejecting the sync

    "CHASE_INTERVAL_SEC": 4,
    "FILL_TIMEOUT_SEC": 60,
    "SCAN_INTERVAL_SEC": 14400,
    "RECONCILE_INTERVAL_SEC": 3600
}


def is_market_open() -> bool:
    est = pytz.timezone('US/Eastern')
    now = datetime.now(est)
    if now.weekday() >= 5: return False
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close


class LEAPSExecutionEngine:
    def __init__(self, ib: IB, supabase: Client, gmail_user: str = None, gmail_pass: str = None,
                 destination_email: str = None):
        self.ib = ib
        self.ib.errorEvent += handle_ib_error
        self.supabase = supabase
        self.net_liquidation = 0.0
        self.gmail_user = gmail_user
        self.gmail_pass = gmail_pass
        self.destination_email = destination_email
        self.open_trades_count = 0
        self.last_scan_time = None
        self.last_reconcile_time = None

        self.instance_id = str(uuid.uuid4())
        print(f"-> Instance ID generated: {self.instance_id}")
        self.claim_execution_lock()

    def claim_execution_lock(self):
        print("-> Claiming exclusive execution lock in Supabase...")
        try:
            self.supabase.table('leaps_system_state').upsert({
                'id': 1,
                'active_instance_id': self.instance_id,
                'updated_at': datetime.now().isoformat()
            }).execute()
        except Exception as e:
            print(f"⚠️ [LOCK ERROR] Failed to claim lock: {e}")

    def check_execution_lock(self):
        try:
            response = self.supabase.table('leaps_system_state').select('active_instance_id').eq('id', 1).execute()
            if response.data:
                current_lock = response.data[0].get('active_instance_id')
                if current_lock and current_lock != self.instance_id:
                    print(f"\n🚨 FATAL: Another machine (ID: {current_lock}) has claimed the execution lock!")
                    print("🚨 Shutting down this instance immediately to prevent duplicate trading.")
                    if self.ib.isConnected():
                        self.ib.disconnect()
                    os._exit(0)
        except Exception as e:
            print(f"⚠️ [LOCK CHECK ERROR] Failed to verify lock: {e}")

    def print_terminal_dashboard(self):
        est = pytz.timezone('US/Eastern')
        current_time = datetime.now(est).strftime('%Y-%m-%d %H:%M:%S ET')
        market_status = "🟢 OPEN" if is_market_open() else "🔴 CLOSED"

        strategy_budget = self.net_liquidation * CONFIG["MAX_STRATEGY_ALLOCATION"]
        strategy_used = getattr(self, 'strategy_used_capital', 0.0)
        strategy_avail = max(0.0, strategy_budget - strategy_used)
        max_trade_cap = strategy_budget * CONFIG["MAX_TRADE_ALLOCATION"]
        account_avail = getattr(self, 'available_funds', 0.0)

        print("\n" + "=" * 65)
        print(f" 🤖 QUANTITATIVE ACTIVE-CHASER LEAPS ENGINE | {current_time}")
        print("=" * 65)
        print(f" Market Status    : {market_status}")
        print(f" Net Liquidation  : ${self.net_liquidation:,.2f}")
        print(f" Account Avail    : ${account_avail:,.2f}")
        print(f" Open Positions   : {self.open_trades_count} / {CONFIG['MAX_OPEN_POSITIONS']} active LEAPS")
        print(f" Stop Loss Mode   : {CONFIG['STOP_LOSS_TYPE']} BASE")
        print("-" * 65)
        print(f" Strategy Budget  : ${strategy_budget:,.2f} ({CONFIG['MAX_STRATEGY_ALLOCATION'] * 100:.0f}%)")
        print(f" Strategy Used    : ${strategy_used:,.2f}")
        print(f" Strategy Avail   : ${strategy_avail:,.2f}")
        print(f" Max Per-Trade Cap: ${max_trade_cap:,.2f}")
        print("=" * 65)

    async def print_holdings_status(self):
        res = await asyncio.to_thread(
            lambda: self.supabase.table("leaps_dip_strategy_ledger").select("*").eq("status", "OPEN").execute())
        open_trades = res.data

        print("\n" + "=" * 85)
        print(" 📊 CURRENT HOLDINGS STATUS (LEAP & UNDERLYING)")
        print("=" * 85)

        if not open_trades:
            print(" No open positions at this time.")
            print("=" * 85 + "\n")
            return

        print(
            f"{'Sym':<5} | {'Qty':<3} | {'L-Cost':<8} | {'L-Cur':<8} | {'L-P&L':<7} | {'S-Cost':<8} | {'S-Cur':<8} | {'S-P&L':<7}")
        print("-" * 85)

        for pos in open_trades:
            try:
                leaps = Contract(conId=pos['leaps_conid'], exchange='SMART')
                stock = Stock(pos['symbol'], 'SMART', 'USD')
                await self.ib.qualifyContractsAsync(leaps, stock)

                tickers = await self.ib.reqTickersAsync(leaps, stock)
                leaps_ticker = next((t for t in tickers if t.contract.conId == leaps.conId), None)
                stock_ticker = next((t for t in tickers if t.contract.conId == stock.conId), None)

                curr_leap_px = leaps_ticker.marketPrice() if leaps_ticker else 0
                if pd.isna(curr_leap_px) or curr_leap_px <= 0:
                    if leaps_ticker and leaps_ticker.bid and leaps_ticker.ask:
                        curr_leap_px = (leaps_ticker.bid + leaps_ticker.ask) / 2.0
                    else:
                        curr_leap_px = leaps_ticker.close if leaps_ticker else 0

                l_cost = pos['net_cost_basis']
                l_cur_str = f"${curr_leap_px:.2f}" if curr_leap_px > 0 else "N/A"
                l_pnl_str = "N/A"
                if curr_leap_px > 0 and l_cost > 0:
                    l_pnl = ((curr_leap_px - l_cost) / l_cost) * 100
                    l_pnl_str = f"{l_pnl:>+6.1f}%"

                curr_stk_px = stock_ticker.marketPrice() if stock_ticker else 0
                if pd.isna(curr_stk_px) or curr_stk_px <= 0:
                    curr_stk_px = stock_ticker.close if stock_ticker else 0

                s_cost = pos.get('stock_entry_price')
                s_cost_str = f"${s_cost:.2f}" if s_cost else "N/A"
                s_cur_str = f"${curr_stk_px:.2f}" if curr_stk_px > 0 else "N/A"
                s_pnl_str = "N/A"

                if s_cost and curr_stk_px > 0:
                    s_pnl = ((curr_stk_px - s_cost) / s_cost) * 100
                    s_pnl_str = f"{s_pnl:>+6.1f}%"

                print(
                    f"{pos['symbol']:<5} | {pos['qty']:<3} | ${l_cost:<7.2f} | {l_cur_str:<8} | {l_pnl_str:<7} | {s_cost_str:<8} | {s_cur_str:<8} | {s_pnl_str:<7}")
            except Exception as e:
                print(f"{pos['symbol']:<5} | Error loading pricing: {e}")
        print("=" * 85 + "\n")

    def send_email_notification(self, subject: str, body: str):
        if not self.gmail_user or not self.gmail_pass or not self.destination_email: return
        try:
            msg = MIMEText(body)
            msg['Subject'] = subject
            msg['From'] = self.gmail_user
            msg['To'] = self.destination_email
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(self.gmail_user, self.gmail_pass)
                server.send_message(msg)
        except Exception as e:
            print(f"❌ [EMAIL FAILED] {e}")

    async def update_account_state(self):
        print("🔄 [SYSTEM] Fetching latest account balances from IBKR...")
        account_values = self.ib.accountValues()

        raw_net_liq = 0.0
        raw_avail = 0.0
        account_currency = 'USD'

        # 1. Grab the balances exactly like the original code, but save the currency
        for val in account_values:
            if val.tag == 'NetLiquidation':
                raw_net_liq = float(val.value)
                account_currency = val.currency  # Likely 'ILS'
            elif val.tag == 'AvailableFunds':
                raw_avail = float(val.value)

        if raw_avail == 0.0:
            for val in account_values:
                if val.tag == 'TotalCashValue' and val.currency == account_currency:
                    raw_avail = float(val.value)

        # 2. If it's already USD, no conversion needed. If not, convert it.
        if account_currency == 'USD':
            self.net_liquidation = raw_net_liq
            self.available_funds = raw_avail
        else:
            usd_fx_rate = None

            # Try to get the FX rate from IBKR
            for val in account_values:
                if val.tag == 'ExchangeRate' and val.currency == 'USD':
                    usd_fx_rate = float(val.value)
                    break

            # Fallback to Yahoo Finance if IBKR doesn't provide it
            if not usd_fx_rate or usd_fx_rate <= 0:
                print(f"⚠️ [SYSTEM] USD Exchange Rate missing from IBKR. Fetching USD/{account_currency} from Yahoo...")
                try:
                    # Dynamically build the ticker, e.g., 'USDILS=X'
                    ticker_str = f"USD{account_currency}=X" if account_currency != 'BASE' else "USDILS=X"
                    usd_fx_rate = await asyncio.to_thread(lambda: yf.Ticker(ticker_str).fast_info['lastPrice'])
                except Exception:
                    usd_fx_rate = 3.75  # Absolute failsafe for ILS

            print(f"💱 [SYSTEM] Applied Exchange Rate: 1 USD = {usd_fx_rate:.4f} {account_currency}")

            # Apply the math to get accurate USD limits
            self.net_liquidation = raw_net_liq / usd_fx_rate
            self.available_funds = raw_avail / usd_fx_rate

        # 3. Update active trades math
        res = await asyncio.to_thread(
            lambda: self.supabase.table("leaps_dip_strategy_ledger").select("net_cost_basis, qty").eq("status",
                                                                                                      "OPEN").execute())

        self.open_trades_count = len(res.data)
        self.strategy_used_capital = sum(float(row['net_cost_basis']) * int(row['qty']) * 100 for row in res.data)

    async def active_chase_order(self, contract: Contract, action: str, qty: int, initial_mid_price: float,
                                 slippage_pct: float):
        ticker = self.ib.reqMktData(contract, "", False, False)
        await asyncio.sleep(0.5)

        start_time = time.time()
        timeout = float(CONFIG["FILL_TIMEOUT_SEC"])
        current_interval = float(CONFIG["CHASE_INTERVAL_SEC"])
        last_chase_time = start_time - current_interval

        current_limit = round(initial_mid_price, 2)
        order = LimitOrder(action, qty, current_limit)
        order.tif = "DAY"
        trade = self.ib.placeOrder(contract, order)

        while not trade.isDone():
            now = time.time()
            elapsed = now - start_time

            if elapsed > timeout:
                print(f"⏳ [TIMEOUT] Order did not fill within {timeout}s. Canceling.")
                self.ib.cancelOrder(order)
                self.ib.cancelMktData(contract)
                return None

            if now - last_chase_time >= current_interval:
                last_chase_time = now
                current_interval = min(current_interval * 1.2, 5.0)

                live_bid = ticker.bid if (ticker.bid and ticker.bid > 0) else initial_mid_price
                live_ask = ticker.ask if (ticker.ask and ticker.ask > 0) else initial_mid_price
                live_mid = (live_bid + live_ask) / 2.0

                if action == "BUY":
                    dynamic_worst_price = live_ask * (1.0 + slippage_pct)
                    start_price = live_mid
                else:
                    dynamic_worst_price = live_bid * (1.0 - slippage_pct)
                    start_price = live_mid

                progress = min(elapsed / timeout, 1.0)

                if action == "BUY":
                    calculated_limit = start_price + ((dynamic_worst_price - start_price) * progress)
                else:
                    calculated_limit = start_price - ((start_price - dynamic_worst_price) * progress)

                new_limit = round(calculated_limit, 2)

                if action == "BUY":
                    new_limit = min(new_limit, round(dynamic_worst_price, 2))
                else:
                    new_limit = max(new_limit, round(dynamic_worst_price, 2))

                if trade.order.lmtPrice != new_limit:
                    trade.order.lmtPrice = new_limit
                    print(
                        f"🔥 [CHASING] {action} {contract.localSymbol} -> ${new_limit:.2f} (Live Mid: ${live_mid:.2f}, Bid: ${live_bid:.2f}, Ask: ${live_ask:.2f})")
                    self.ib.placeOrder(contract, trade.order)

            await asyncio.sleep(0.2)

        self.ib.cancelMktData(contract)
        return trade

    def get_scan_tickers(self) -> dict:
        """
        Returns a dictionary mapping ticker to a boolean indicating if it belongs to a target sector:
        { 'AAPL': True, 'JPM': True, 'XOM': False, ... }
        """
        tickers_map = {}
        headers = {'User-Agent': 'Mozilla/5.0'}
        target_sectors = ['Information Technology', 'Financials', 'Communication Services', 'Industrials']

        print("🌐 [SCANNER] Downloading S&P 500 roster from Wikipedia...")
        try:
            url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            response_sp = requests.get(url_sp, headers=headers, timeout=10)
            tables_sp = pd.read_html(io.StringIO(response_sp.text))

            for t in tables_sp:
                sym_col = next((c for c in t.columns if 'symbol' in str(c).lower() or 'ticker' in str(c).lower()), None)
                if sym_col:
                    for _, row in t.iterrows():
                        ticker = str(row[sym_col]).replace('.', '-')
                        is_target = True

                        if 'GICS Sector' in t.columns:
                            sector = row['GICS Sector']
                            sub_ind = row.get('GICS Sub-Industry', '')
                            is_target = (sector in target_sectors) and (sub_ind != 'Airlines')

                        # If strict filtering is ON, skip non-target sectors entirely
                        if CONFIG.get("ENABLE_SECTOR_FILTER", True) and not is_target:
                            continue

                        tickers_map[ticker] = is_target
                    break
        except Exception as e:
            print(f"⚠️️ [SCANNER] S&P 500 fetch failed: {e}")

        if CONFIG.get("SCAN_NASDAQ_100", False):
            print("🌐 [SCANNER] Downloading NASDAQ 100 roster from Wikipedia...")
            try:
                url_ndx = 'https://en.wikipedia.org/wiki/Nasdaq-100'
                response_ndx = requests.get(url_ndx, headers=headers, timeout=10)
                tables_ndx = pd.read_html(io.StringIO(response_ndx.text))

                for t in tables_ndx:
                    sym_col = next((c for c in t.columns if 'ticker' in str(c).lower() or 'symbol' in str(c).lower()),
                                   None)
                    if sym_col:
                        for _, row in t.iterrows():
                            ticker = str(row[sym_col]).replace('.', '-')
                            is_target = True

                            if 'GICS Sector' in t.columns:
                                sector = row['GICS Sector']
                                is_target = (sector in target_sectors)

                            if CONFIG.get("ENABLE_SECTOR_FILTER", True) and not is_target:
                                continue

                            tickers_map[ticker] = is_target
                        break
            except Exception as e:
                print(f"⚠️ [SCANNER] NASDAQ 100 fetch failed: {e}")

        if not tickers_map:
            default_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA']
            return {t: True for t in default_tickers}

        return tickers_map

    def validate_technicals(self, df_hist: pd.Series) -> bool:
        try:
            hist = df_hist.dropna()

            # Dynamically set minimum history based on the SMA toggle
            min_len = 200 if CONFIG.get("REQUIRE_200_SMA_UPTREND", True) else 50
            if len(hist) < min_len: return False

            current_price = hist.iloc[-1]

            # 1. Long-Term Trend Check (Secular Bull Market)
            if CONFIG.get("REQUIRE_200_SMA_UPTREND", True):
                sma_200 = hist.rolling(window=200).mean().iloc[-1]
                if current_price <= sma_200:
                    return False

            # 2. Short-Term Pullback Check (The Dip)
            sma_50 = hist.rolling(window=50).mean().iloc[-1]
            if current_price >= sma_50:
                return False

            # 3. Exhaustion Check
            delta = hist.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs)).iloc[-1]
            if rsi >= 45: return False

            # 4. Momentum Reversal Check
            exp1 = hist.ewm(span=12, adjust=False).mean()
            exp2 = hist.ewm(span=26, adjust=False).mean()
            macd = exp1 - exp2
            signal = macd.ewm(span=9, adjust=False).mean()
            if macd.iloc[-1] <= signal.iloc[-1]: return False

            return True
        except Exception:
            return False

    def validate_fundamentals(self, ticker: str) -> bool:
        try:
            time.sleep(random.uniform(0.1, 0.4))
            info = yf.Ticker(ticker).info
            eps = info.get('trailingEps', 0)
            fwd_pe = info.get('forwardPE', 999)
            if eps <= 0 or fwd_pe > 40: return False
            return True
        except Exception:
            return False

    async def get_zacks_score_async(self, ticker: str) -> int:
        url = f"https://quote-feed.zacks.com/index.php?t={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=5) as res:
                    if res.status == 200:
                        data = await res.json()
                        if ticker in data and "zacks_rank" in data[ticker]:
                            rank_str = data[ticker]["zacks_rank"]
                            if rank_str and rank_str[0].isdigit():
                                return int(rank_str[0])
            return 99
        except Exception as e:
            print(f"⚠️ [ZACKS] Failed to fetch rank for {ticker}: {e}")
            return 99

    async def execute_leaps(self, symbol: str) -> bool:
        print(f"\n🛒 [TRADE] Initiating Active LEAPS purchase for {symbol}...")
        stock = Stock(symbol, 'SMART', 'USD')
        await self.ib.qualifyContractsAsync(stock)
        [stock_ticker] = await self.ib.reqTickersAsync(stock)

        stock_price = stock_ticker.marketPrice()
        if math.isnan(stock_price) or stock_price <= 0: stock_price = stock_ticker.close
        if math.isnan(stock_price) or stock_price <= 0:
            print(f"⚠️ [TRADE] Could not determine stock price for {symbol}.")
            return False

        chains = await self.ib.reqSecDefOptParamsAsync(stock.symbol, '', stock.secType, stock.conId)
        if not chains: return False
        chain = next((c for c in chains if c.exchange == 'SMART'), chains[0])
        now = datetime.now()
        if not chain.expirations: return False

        target_dte = CONFIG["LEAPS_DTE_TARGET"]
        leaps_exp = min(chain.expirations,
                        key=lambda exp: abs((datetime.strptime(exp, '%Y%m%d') - now).days - target_dte))
        actual_dte = (datetime.strptime(leaps_exp, '%Y%m%d') - now).days

        details = await self.ib.reqContractDetailsAsync(Option(symbol, leaps_exp, exchange='SMART'))
        strikes = [d.contract.strike for d in details if d.contract.right == 'C']
        if not strikes: return False

        print("🧮 [GREEKS] Fetching live Greeks to find accurate 0.80 Delta...")
        itm_strikes = sorted([s for s in strikes if s < stock_price], reverse=True)
        if not itm_strikes:
            print(f"⚠️ [TRADE] {symbol} has no ITM strikes available for {leaps_exp}. Skipping.")
            return False

        target_estimate = stock_price * 0.80
        test_strikes = sorted(itm_strikes, key=lambda x: abs(x - target_estimate))[:5]

        test_contracts = [Option(symbol, leaps_exp, strike=s, right='C', exchange='SMART') for s in test_strikes]
        if not test_contracts: return False

        test_contracts = await self.ib.qualifyContractsAsync(*test_contracts)
        tickers = await self.ib.reqTickersAsync(*test_contracts)

        await asyncio.sleep(2)

        best_contract = None
        best_delta_diff = 999
        for t in tickers:
            if t.modelGreeks and t.modelGreeks.delta:
                diff = abs(abs(t.modelGreeks.delta) - CONFIG["LEAPS_DELTA_TARGET"])
                if diff < best_delta_diff:
                    best_delta_diff = diff
                    best_contract = t.contract

        if not best_contract:
            closest_strike = min(strikes, key=lambda x: abs(x - target_estimate))
            best_contract = Option(symbol, leaps_exp, strike=closest_strike, right='C', exchange='SMART')
            await self.ib.qualifyContractsAsync(best_contract)
            print("⚠️ [GREEKS] IBKR ModelGreeks unavailable. Falling back to 20% DITM math estimate.")

        leaps_contract = best_contract
        closest_strike = leaps_contract.strike
        print(f"🎯 [STRIKE] {symbol} Px: ${stock_price:.2f} | Selected Strike: ${closest_strike:.2f}")

        [opt_ticker] = await self.ib.reqTickersAsync(leaps_contract)
        bid = opt_ticker.bid if (opt_ticker.bid and opt_ticker.bid > 0) else opt_ticker.close
        ask = opt_ticker.ask if (opt_ticker.ask and opt_ticker.ask > 0) else opt_ticker.close

        if not bid or not ask or bid <= 0 or ask <= 0:
            print(f"⚠️ [TRADE] Missing liquidity for {symbol} contract. Aborting.")
            return False

        spread_pct = (ask - bid) / bid
        if spread_pct > CONFIG.get("MAX_LEAP_SPREAD_PCT", 0.20):
            print(
                f"🛑 [TRADE ABORT] {symbol} LEAP Spread is too wide ({spread_pct * 100:.1f}%). Limit is {CONFIG['MAX_LEAP_SPREAD_PCT'] * 100:.1f}%.")
            return False

        mid_price = (bid + ask) / 2.0
        ceiling_limit = mid_price * (1.0 + CONFIG["MAX_BUY_SLIPPAGE_PCT"])

        strategy_budget = self.net_liquidation * CONFIG["MAX_STRATEGY_ALLOCATION"]
        strategy_used = getattr(self, 'strategy_used_capital', 0.0)
        strategy_avail = max(0.0, strategy_budget - strategy_used)
        trade_limit = strategy_budget * CONFIG["MAX_TRADE_ALLOCATION"]
        account_avail = getattr(self, 'available_funds', 0.0)

        actual_trade_allocation = min(trade_limit, strategy_avail, account_avail)

        if actual_trade_allocation <= 0:
            print(
                f"🛑 [ABORT] {symbol}: 0 capital allocated. Trade Limit: ${trade_limit:.2f}, Strategy Avail: ${strategy_avail:.2f}, Account Avail: ${account_avail:.2f}")
            return False

        qty = int(actual_trade_allocation // (ceiling_limit * 100))
        if qty < 1:
            print(f"🛑 [ABORT] {symbol}: Not enough capital for 1 contract.")
            return False

        print(
            f"⚡ [ACTIVE CHASER] Placing BUY {qty}x {symbol} | Starting at Mid: ${mid_price:.2f} -> Ceiling: ${ceiling_limit:.2f}")

        self.strategy_used_capital += (ceiling_limit * qty * 100)
        self.available_funds -= (ceiling_limit * qty * 100)

        trade = await self.active_chase_order(leaps_contract, "BUY", qty, mid_price, CONFIG["MAX_BUY_SLIPPAGE_PCT"])

        if not trade or not trade.isDone():
            self.strategy_used_capital -= (ceiling_limit * qty * 100)
            self.available_funds += (ceiling_limit * qty * 100)
            return False

        fill_price = trade.orderStatus.avgFillPrice or mid_price
        print(f"🟢 [TRADE FILLED] Bought {qty}x LEAPS for {symbol} @ ${fill_price:.2f}")

        self.strategy_used_capital -= (ceiling_limit * qty * 100)
        self.strategy_used_capital += (fill_price * qty * 100)
        self.available_funds += (ceiling_limit * qty * 100)
        self.available_funds -= (fill_price * qty * 100)

        await asyncio.to_thread(lambda: self.supabase.table("leaps_dip_strategy_ledger").insert({
            "symbol": symbol, "strategy": "LONG_LEAPS", "leaps_conid": leaps_contract.conId,
            "net_cost_basis": fill_price, "high_water_mark": fill_price, "qty": qty,
            "stock_entry_price": stock_price,
            "status": "OPEN", "timestamp": datetime.now().isoformat()
        }).execute())

        await asyncio.to_thread(self.send_email_notification, f"TRADE OPENED: LEAPS on {symbol}",
                                f"Bought LEAPS on {symbol}.\nExpiration: {leaps_exp} ({actual_dte} DTE)\nStrike: ${closest_strike}\nAvg Fill Price: ${fill_price:.2f}\nQuantity: {qty}")
        return True

    async def reconcile_portfolio(self):
        est = pytz.timezone('US/Eastern')
        now_time = datetime.now(est).time()

        if dt_time(9, 30) <= now_time <= dt_time(9, 35):
            print("⏳ [RECONCILE] Skipping sync during opening 5-minute volatility to prevent Ghost Wipes.")
            return

        print("🔍 [RECONCILE] Syncing IBKR portfolio with Supabase ledger...")
        positions = await self.ib.reqPositionsAsync()

        if not positions:
            print("⚠️ [RECONCILE] IBKR returned 0 positions. Skipping sync to protect against Ghost Wipe.")
            return

        active_conids = {p.contract.conId for p in positions if p.position != 0}

        res = await asyncio.to_thread(
            lambda: self.supabase.table("leaps_dip_strategy_ledger").select("*").eq("status", "OPEN").execute())

        db_open_trades = res.data
        self.open_trades_count = len(db_open_trades)

        # Identify which database positions are missing from the live IBKR portfolio
        missing_trades = [pos for pos in db_open_trades if int(pos['leaps_conid']) not in active_conids]

        # Prevent partial API return "Ghost Wipes"
        max_drops = CONFIG.get("MAX_UNEXPECTED_DROPS", 1)
        if len(missing_trades) > max_drops:
            print(f"⚠️ [RECONCILE] {len(missing_trades)} positions missing simultaneously from IBKR.")
            print(
                f"⚠️️ [RECONCILE] Exceeds MAX_UNEXPECTED_DROPS ({max_drops}). Suspected partial API load. Skipping sync.")
            return

        for pos in missing_trades:
            print(f"🧹 [RECONCILE] {pos['symbol']} not found in IBKR. Marking as CLOSED in db.")
            await asyncio.to_thread(
                lambda p=pos: self.supabase.table("leaps_dip_strategy_ledger").update({"status": "CLOSED"}).eq("id", p[
                    'id']).execute())
            self.open_trades_count -= 1

    async def process_exit(self, leaps: Contract, pos: dict, current_value: float, trigger_reason: str):
        print(f"🚨 [EXIT TRIGGERED] {trigger_reason} hit for {pos['symbol']}. Initiating SELL Chaser...")
        trade = await self.active_chase_order(leaps, "SELL", pos['qty'], current_value, CONFIG["MAX_SELL_SLIPPAGE_PCT"])

        if trade and trade.isDone():
            exit_px = trade.orderStatus.avgFillPrice or current_value
            await asyncio.to_thread(
                lambda p=pos: self.supabase.table("leaps_dip_strategy_ledger").update({"status": "CLOSED"}).eq("id", p[
                    'id']).execute())
            print(f"✅ [EXIT FILLED] Closed {pos['symbol']} @ ${exit_px:.2f} ({trigger_reason})")

            pnl_pct = ((current_value - pos['net_cost_basis']) / pos['net_cost_basis']) * 100
            await asyncio.to_thread(self.send_email_notification, f"TRADE CLOSED: {trigger_reason} on {pos['symbol']}",
                                    f"Closed {pos['symbol']} due to {trigger_reason}.\nExit Price: ${exit_px:.2f}\nP&L: {pnl_pct:+.2f}%")

    async def enforce_risk_management(self):
        res = await asyncio.to_thread(
            lambda: self.supabase.table("leaps_dip_strategy_ledger").select("*").eq("status", "OPEN").execute())
        if not res.data: return

        exit_tasks = []

        for pos in res.data:
            leaps = Contract(conId=pos['leaps_conid'], secType='OPT', exchange='SMART')
            stock = Stock(pos['symbol'], 'SMART', 'USD')
            await self.ib.qualifyContractsAsync(leaps, stock)

            tickers = await self.ib.reqTickersAsync(leaps, stock)
            leaps_ticker = next((t for t in tickers if t.contract.conId == leaps.conId), None)
            stock_ticker = next((t for t in tickers if t.contract.conId == stock.conId), None)

            current_leap_mid = leaps_ticker.marketPrice() if leaps_ticker else 0
            if pd.isna(current_leap_mid) or current_leap_mid <= 0:
                if leaps_ticker and leaps_ticker.bid and leaps_ticker.ask:
                    current_leap_mid = (leaps_ticker.bid + leaps_ticker.ask) / 2.0
                else:
                    current_leap_mid = leaps_ticker.close if leaps_ticker else 0

            if pd.isna(current_leap_mid) or current_leap_mid <= 0: continue

            current_stock_value = stock_ticker.marketPrice() if stock_ticker else 0
            if pd.isna(current_stock_value) or current_stock_value <= 0:
                current_stock_value = stock_ticker.close if stock_ticker else 0

            cost_basis = pos['net_cost_basis']
            hwm = max(pos['high_water_mark'], current_leap_mid)

            if hwm > pos['high_water_mark']:
                if hwm > (pos['high_water_mark'] * 1.50):
                    print(
                        f"⚠️ [RISK] Ignoring abnormal LEAP price spike on {pos['symbol']} (${pos['high_water_mark']} ->${hwm})")
                    hwm = pos['high_water_mark']
                else:
                    await asyncio.to_thread(
                        lambda p=pos, h=hwm: self.supabase.table("leaps_dip_strategy_ledger").update(
                            {"high_water_mark": h}).eq("id", p['id']).execute())

            live_bid = leaps_ticker.bid if (
                    leaps_ticker and leaps_ticker.bid and leaps_ticker.bid > 0) else current_leap_mid
            realizable_profit_pct = (live_bid - cost_basis) / cost_basis
            drawdown_from_hwm = (hwm - current_leap_mid) / hwm if hwm > 0 else 0

            trigger_reason = None

            if realizable_profit_pct >= CONFIG["TAKE_PROFIT_PCT"]:
                trigger_reason = "Take Profit"
            elif drawdown_from_hwm >= CONFIG["TRAILING_STOP_PCT"] and realizable_profit_pct > 0.005:
                trigger_reason = "Trailing Stop"
            else:
                if CONFIG["STOP_LOSS_TYPE"] == "UNDERLYING":
                    stock_cost = pos.get('stock_entry_price')
                    if stock_cost and current_stock_value > 0:
                        stock_profit_pct = (current_stock_value - stock_cost) / stock_cost
                        if stock_profit_pct <= -CONFIG["UNDERLYING_STOP_LOSS_PCT"]:
                            trigger_reason = f"Underlying Stop Loss ({stock_profit_pct * 100:.1f}%)"
                else:
                    if realizable_profit_pct <= -CONFIG["STOP_LOSS_PCT"]:
                        trigger_reason = f"LEAP Stop Loss ({realizable_profit_pct * 100:.1f}%)"

            if trigger_reason:
                exit_tasks.append(self.process_exit(leaps, pos, current_leap_mid, trigger_reason))

        if exit_tasks:
            await asyncio.gather(*exit_tasks)

    async def run_loop(self):
        startup_date = datetime.now().date()
        est = pytz.timezone('US/Eastern')

        while True:
            try:
                self.check_execution_lock()

                if datetime.now().date() > startup_date:
                    print("🔄 [SYSTEM] Day rolled over. Resetting connection for daily broker maintenance...")
                    break

                if not self.ib.isConnected():
                    print("⚠️ [SYSTEM] Connection lost. Reconnecting...")
                    break

                await self.update_account_state()
                self.print_terminal_dashboard()
                await self.print_holdings_status()

                if not is_market_open():
                    print("💤 Market is closed. Engine sleeping for 15 minutes...")
                    await asyncio.sleep(900)
                    continue

                now = datetime.now(est)

                if not self.last_reconcile_time or (now - self.last_reconcile_time).total_seconds() >= CONFIG[
                    "RECONCILE_INTERVAL_SEC"]:
                    await self.reconcile_portfolio()
                    self.last_reconcile_time = datetime.now(est)

                await self.enforce_risk_management()

                current_cooldown = CONFIG["SCAN_INTERVAL_SEC"] if self.open_trades_count >= CONFIG[
                    "MAX_OPEN_POSITIONS"] else 900

                time_to_scan = False
                if not self.last_scan_time:
                    time_to_scan = True
                elif (now - self.last_scan_time).total_seconds() >= current_cooldown:
                    time_to_scan = True

                if not time_to_scan:
                    time_left = current_cooldown - (now - self.last_scan_time).total_seconds()
                    print(f"⏳ [SCANNER] Resting. Next market scan in {int(time_left // 60)}m {int(time_left % 60)}s...")
                else:
                    if now.time() < dt_time(10, 30):
                        print("⏳ [SCANNER] Waiting until 10:30 ET to run scan...")
                    else:
                        slots_available = CONFIG["MAX_OPEN_POSITIONS"] - self.open_trades_count

                        if slots_available <= 0:
                            print(
                                f"🛑 [SCANNER SKIPPED] Portfolio full. Maximum active trades ({CONFIG['MAX_OPEN_POSITIONS']}) reached.")
                            self.last_scan_time = datetime.now(est)
                        else:
                            print(f"🕵️ [SCANNER] Initiating market scan (Available Slots: {slots_available})...")

                            # --- 1. Fetch map and get tickers list ---
                            ticker_sector_map = await asyncio.to_thread(self.get_scan_tickers)
                            tickers = list(ticker_sector_map.keys())

                            res = await asyncio.to_thread(
                                lambda: self.supabase.table("leaps_dip_strategy_ledger").select("symbol").eq("status",
                                                                                                             "OPEN").execute())
                            db_open_symbols = {row['symbol'] for row in res.data}

                            portfolio = self.ib.portfolio()
                            ib_held_symbols = {p.contract.symbol for p in portfolio if p.position > 0}

                            open_trades = self.ib.openTrades()
                            ib_pending_symbols = {t.contract.symbol for t in open_trades if
                                                  t.orderStatus.status not in ["Cancelled", "Filled"]}

                            all_excluded_symbols = db_open_symbols.union(ib_held_symbols).union(ib_pending_symbols)
                            tickers = [t for t in tickers if t not in all_excluded_symbols]

                            # --- 2. Calculate current non-target slot usage ---
                            max_non_target_slots = int(
                                CONFIG["MAX_OPEN_POSITIONS"] * CONFIG.get("MAX_NON_TARGET_SECTOR_PCT", 0.20))
                            current_non_target_held = sum(
                                1 for sym in db_open_symbols if not ticker_sector_map.get(sym, True))

                            print("⚡ [SCANNER] Bulk downloading historical data to prevent API bans...")

                            # Period updated from 6mo to 1y to guarantee 200+ trading days for the SMA requirement
                            hist_data = await asyncio.to_thread(yf.download, tickers, period="1y", progress=False)
                            close_data = hist_data['Close'] if 'Close' in hist_data else hist_data

                            tech_survivors = []
                            for t in tickers:
                                if t in close_data.columns and self.validate_technicals(close_data[t]):
                                    tech_survivors.append(t)

                            print(f"📊 [SCANNER] {len(tech_survivors)} passed technicals. Checking fundamentals...")

                            candidates = []
                            if tech_survivors:
                                loop = asyncio.get_running_loop()
                                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                                    tasks = [loop.run_in_executor(executor, self.validate_fundamentals, t) for t in
                                             tech_survivors]
                                    results = await asyncio.gather(*tasks)

                                fun_survivors = [tech_survivors[i] for i, passed in enumerate(results) if passed]

                                if fun_survivors:
                                    print("🐢 [SCANNER] Querying Zacks ranks for survivors...")
                                    for ticker in fun_survivors:
                                        z_rank = await self.get_zacks_score_async(ticker)

                                        if z_rank <= CONFIG["MAX_ZACKS_RANK"]:
                                            print(f"   ⭐ {ticker} passed with Zacks Rank {z_rank}")
                                            candidates.append((ticker, z_rank))

                            if candidates:
                                candidates.sort(key=lambda x: x[1])

                                print(f"🔥 [SCANNER FINAL] Found {len(candidates)} setups. Sorting by Zacks Rank...")
                                for ticker, rank in candidates:
                                    if self.open_trades_count >= CONFIG["MAX_OPEN_POSITIONS"]:
                                        print("🛑 [CAPACITY] Portfolio is full. Halting new entries.")
                                        break

                                    # --- 3. Check Sector Capacity Constraints ---
                                    is_target = ticker_sector_map.get(ticker, True)
                                    if not is_target and current_non_target_held >= max_non_target_slots:
                                        print(
                                            f"   -> ⏭️ Skipping {ticker} (Zacks Rank {rank}) - Non-target sector capacity reached ({current_non_target_held}/{max_non_target_slots})")
                                        continue

                                    print(
                                        f"   -> 🚀 Executing top candidate {ticker} (Zacks Rank {rank} | Target Sector: {is_target})")
                                    success = await self.execute_leaps(ticker)

                                    if success:
                                        self.open_trades_count += 1
                                        if not is_target:
                                            current_non_target_held += 1
                                    await asyncio.sleep(3)
                            else:
                                print("🙅 [SCANNER] No stocks passed the final filters this cycle.")

                            self.last_scan_time = datetime.now(est)
                            print(f"⏳ [SYSTEM] Deep scan complete. Next scan in {current_cooldown // 60} minutes.")

                await asyncio.sleep(60)

            except Exception as e:
                print(f"❌ [LOOP ERROR] {e}")
                await asyncio.sleep(60)


def load_credentials_from_file(filepath: str):
    try:
        with open(filepath, 'r') as file:
            lines = [line.strip() for line in file if line.strip() and not line.startswith('#')]
        sb_url = lines[0]
        sb_key = lines[1]
        gmail_user = lines[2] if len(lines) > 2 else None
        gmail_pass = lines[3] if len(lines) > 3 else None
        destination_email = lines[4] if len(lines) > 4 else gmail_user
        return sb_url, sb_key, gmail_user, gmail_pass, destination_email
    except Exception as e:
        print(f"❌ [CREDENTIALS ERROR] {e}")
        raise


async def connect_with_retries_async(ib: IB, host: str, port: int, client_id: int, max_retries: int = 5,
                                     retry_delay: int = 15):
    for attempt in range(1, max_retries + 1):
        try:
            print(f"🔌 [IBKR] Connecting to TWS/Gateway on port {port} (Attempt {attempt}/{max_retries})...")
            await ib.connectAsync(host, port, clientId=client_id, timeout=15)
            print("✅ [IBKR] Connected Successfully!")
            return True
        except Exception:
            if attempt < max_retries: await asyncio.sleep(retry_delay)
    return False


async def main_watchdog(sb_url: str, sb_key: str, gmail_user: str, gmail_pass: str, dest_email: str):
    supabase_client = create_client(sb_url, sb_key)
    client_id = 99
    while True:
        util.patchAsyncio()
        ib_client = IB()
        connected = await connect_with_retries_async(ib_client, '127.0.0.1', 7496, client_id=client_id, max_retries=10,
                                                     retry_delay=30)
        if not connected:
            await asyncio.sleep(300)
            continue
        bot = LEAPSExecutionEngine(ib=ib_client, supabase=supabase_client, gmail_user=gmail_user, gmail_pass=gmail_pass,
                                   destination_email=dest_email)
        try:
            await bot.run_loop()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"❌ [FATAL ENGINE ERROR] {e}")
        finally:
            if ib_client.isConnected(): ib_client.disconnect()
            await asyncio.sleep(30)
            client_id += 1


if __name__ == "__main__":
    credentials_file = "c:/amit/supabase.txt"
    try:
        url, key, g_user, g_pass, d_email = load_credentials_from_file(credentials_file)
        asyncio.run(main_watchdog(url, key, g_user, g_pass, d_email))
    except KeyboardInterrupt:
        print("\n👋 [SHUTDOWN] Exiting gracefully.")