import datetime
import math
import pytz
import smtplib
import time
import random
import asyncio
import uuid
import sys
from email.message import EmailMessage
from ib_async import IB, Option, Contract, LimitOrder, ComboLeg, Index
from supabase import create_client


class TrueLiveSPXSwingTrader:
    def __init__(self, ib_host='127.0.0.1', ib_port=7496, client_id=3,
                 max_account_risk_pct=0.4, max_risk_per_trade_pct=0.12, max_trades_per_week=2,
                 max_total_active_trades=8, max_contracts_per_trade=10,
                 min_vix=18.0, max_vix=35.0, vix_threshold_high=20.0,
                 vix_threshold_low=13.0, vix_switch_day=3,
                 min_credit_ratio=0.08,
                 gmail_user='', gmail_pass_file='spxpass.txt', notify_email='',
                 creds_file='supabase.txt', fast_profit_hours=240.0,
                 entry_retries=15, entry_retry_delay_sec=5.0,
                 exit_retries=10, exit_retry_delay_sec=5.0,
                 entry_floor_pct=0.75):

        self.ib_host = ib_host
        self.ib_port = ib_port
        self.client_id = client_id

        # Initial Connection
        print("-> Connecting to IBKR...")
        self.ib = IB()
        self.connect_ib()

        self.max_account_risk_pct = max_account_risk_pct
        self.max_risk_per_trade_pct = max_risk_per_trade_pct
        self.max_trades_per_week = max_trades_per_week
        self.max_total_active_trades = max_total_active_trades
        self.max_contracts_per_trade = max_contracts_per_trade
        self.min_credit_ratio = min_credit_ratio
        self.max_vix = max_vix
        self.fast_profit_hours = fast_profit_hours

        self.gmail_user = gmail_user
        self.gmail_app_password = self._init_password(gmail_pass_file)
        self.notify_email = notify_email if notify_email else gmail_user

        self.vix_threshold_high = vix_threshold_high
        self.vix_threshold_low = vix_threshold_low
        self.vix_switch_day = vix_switch_day

        # Execution parameters
        self.entry_retries = entry_retries
        self.entry_retry_delay_sec = entry_retry_delay_sec
        self.exit_retries = exit_retries
        self.exit_retry_delay_sec = exit_retry_delay_sec
        self.entry_floor_pct = entry_floor_pct

        # Internal State Variables
        self.active_spreads = {}
        self.last_trade_date = ""
        self.vix_week_mode = 1
        self.last_week_number = 0
        self.last_signal_time = ""
        self.last_sync_date = ""
        self.stop_loss_strikes = {}  # <--- ADD THIS LINE

        # Liquidity Filters
        self.min_leg_size = 5
        self.max_leg_bid_ask_spread = 1.2
        self.min_daily_volume = 10

        print("-> Connecting to Supabase...")
        self.supabase = self._init_supabase(creds_file)

        print("-> Loading state...")
        self._load_state()
        self._check_week_rotation()

        # ---> HIGHLANDER LOCK INITIATION <---
        self.instance_id = str(uuid.uuid4())
        print(f"-> Instance ID generated: {self.instance_id}")
        self.claim_execution_lock()

        print("-> Engine Initialized Successfully.")

    def connect_ib(self):
        if self.ib.isConnected():
            self.ib.disconnect()
        self.ib.connect(self.ib_host, self.ib_port, clientId=self.client_id)
        self.ib.reqMarketDataType(1)
        print("-> IBKR Connected!")

    def _init_supabase(self, creds_file):
        try:
            with open(creds_file, 'r') as f:
                lines = f.read().splitlines()
                url = lines[0].strip()
                key = lines[1].strip()
            return create_client(url, key)
        except Exception as e:
            print(f"⚠️ DATABASE INIT ERROR: {e}")
            return None

    def _init_password(self, pass_file):
        try:
            with open(pass_file, 'r') as f:
                return f.read().strip()
        except Exception as e:
            print(f"⚠️ EMAIL PASSWORD INIT ERROR: {e}")
            return ""

    def claim_execution_lock(self):
        print("-> Claiming execution lock in Supabase...")
        if self.supabase:
            try:
                self.supabase.table('spx_system_state').update({
                    'active_instance_id': self.instance_id,
                    'updated_at': datetime.datetime.now(pytz.utc).isoformat()
                }).eq('id', 1).execute()
            except Exception as e:
                print(f"⚠️ LOCK CLAIM ERROR: {e}")

    def check_execution_lock(self):
        """Checks if another machine has taken over. If yes, kills this script."""
        if self.supabase:
            try:
                response = self.supabase.table('spx_system_state').select('active_instance_id').eq('id', 1).execute()
                if response.data:
                    current_lock = response.data[0].get('active_instance_id')

                    if current_lock and current_lock != self.instance_id:
                        print(f"\n🚨 FATAL: Another machine (ID: {current_lock}) has claimed the execution lock!")
                        print("🚨 Shutting down this instance immediately to prevent duplicate trading.")
                        if self.ib.isConnected():
                            self.ib.disconnect()
                        sys.exit(0)
            except Exception as e:
                print(f"⚠️ LOCK CHECK ERROR: {e}")

    def _save_state(self):
        state_data = {
            'id': 1,
            'active_spreads': self.active_spreads,
            'last_trade_date': self.last_trade_date,
            'vix_week_mode': self.vix_week_mode,
            'last_week_number': self.last_week_number,
            'last_signal_time': self.last_signal_time,
            'last_sync_date': self.last_sync_date,
            'updated_at': datetime.datetime.now(pytz.utc).isoformat()
        }
        if self.supabase:
            try:
                self.supabase.table('spx_system_state').upsert(state_data).execute()
            except Exception as e:
                print(f"⚠️ DB STATE SAVE ERROR: {e}")

    def _load_state(self):
        if self.supabase:
            try:
                response = self.supabase.table('spx_system_state').select('*').eq('id', 1).execute()
                if response.data:
                    state = response.data[0]
                    self.active_spreads = state.get('active_spreads', {})
                    self.last_trade_date = state.get('last_trade_date', "")
                    self.vix_week_mode = state.get('vix_week_mode', 1)
                    self.last_week_number = state.get('last_week_number', 0)
                    self.last_signal_time = state.get('last_signal_time', "")
                    self.last_sync_date = state.get('last_sync_date', "")
                    return
            except Exception as e:
                print(f"⚠️ DB STATE LOAD ERROR: {e}")

        self.active_spreads = {}
        self.last_trade_date = ""

    def close_orphaned_leg(self, ib_expiration, strike, action, quantity, tc):
        """Sends an aggressive marketable limit order to close a single orphaned leg."""
        contract = Option('SPX', ib_expiration, strike, 'P', 'SMART', tradingClass=tc)
        self.ib.qualifyContracts(contract)

        # Get quick snapshot pricing
        ticker = self.ib.reqMktData(contract, '101,106', snapshot=True)
        self.ib.sleep(2)

        # Determine price based on action (BUY asks, SELL bids)
        price = ticker.ask if action == 'BUY' else ticker.bid
        if math.isnan(price) or price <= 0:
            price = ticker.last if (ticker.last and not math.isnan(ticker.last)) else ticker.close

        if math.isnan(price) or price <= 0:
            price = 5.00  # Arbitrary safety net if data is totally missing

        # Make it an aggressive marketable limit (overpay by 20% to guarantee emergency fill)
        adj_price = price * 1.20 if action == 'BUY' else price * 0.80
        lmt_price = round(adj_price * 20) / 20.0

        print(f"🚨 EMERGENCY CLEANUP: Sending {action} {quantity}x {strike}P at ~${lmt_price:.2f}")
        order = LimitOrder(action, quantity, lmt_price, tif='DAY')
        trade = self.ib.placeOrder(contract, order)

        for _ in range(10):
            self.ib.sleep(1)
            if trade.orderStatus.status == 'Filled':
                print(f"✅ Orphaned leg successfully flattened at ${trade.orderStatus.avgFillPrice:.2f}")
                return

        print("⚠️ Orphan cleanup order pending/unfilled. Please check IBKR manually.")

    def reconcile_portfolio_state(self):
        print(f"\n🔄 RECONCILIATION: Cross-checking DB state against live IBKR portfolio...")
        positions = self.ib.positions()
        spx_options = [p for p in positions if
                       p.contract.symbol == 'SPX' and p.contract.secType == 'OPT' and p.position != 0]

        keys_to_remove = []
        for dict_key in list(self.active_spreads.keys()):
            parts = dict_key.split('_')
            if len(parts) != 3:
                continue
            ib_exp, target_short, target_long = parts[0], float(parts[1]), float(parts[2])

            short_legs = [p for p in spx_options if
                          p.contract.lastTradeDateOrContractMonth == ib_exp and p.contract.strike == target_short and p.position < 0]
            long_legs = [p for p in spx_options if
                         p.contract.lastTradeDateOrContractMonth == ib_exp and p.contract.strike == target_long and p.position > 0]

            short_exists = len(short_legs) > 0
            long_exists = len(long_legs) > 0

            # 1. Both legs manually closed
            if not short_exists and not long_exists:
                print(f"⚠️ {dict_key} is entirely missing from IBKR (Manually closed). Removing from DB.")
                keys_to_remove.append(dict_key)

            # 2. Long leg closed manually, short remains (Dangerous Naked Short!)
            elif short_exists and not long_exists:
                print(f"🚨 NAKED RISK DETECTED: Long leg missing, short {target_short}P remains!")
                qty = abs(short_legs[0].position)
                tc = short_legs[0].contract.tradingClass
                self.close_orphaned_leg(ib_exp, target_short, 'BUY', qty, tc)
                keys_to_remove.append(dict_key)

            # 3. Short leg closed manually, long remains
            elif long_exists and not short_exists:
                print(f"⚠️ ORPHAN DETECTED: Short leg missing, long {target_long}P remains.")
                qty = abs(long_legs[0].position)
                tc = long_legs[0].contract.tradingClass
                self.close_orphaned_leg(ib_exp, target_long, 'SELL', qty, tc)
                keys_to_remove.append(dict_key)

        if keys_to_remove:
            for k in keys_to_remove:
                del self.active_spreads[k]
                parts = k.split('_')
                self.log_transaction('SYNC', parts[0], parts[1], parts[2], 0, 'DB_CLEANUP', 'Removed', 0.0, "",
                                     "Cleaned up due to manual intervention")

            self._save_state()
            print("✅ DB CLEANUP COMPLETE: Engine unlocked for new setups.\n")
        else:
            print("✅ PORTFOLIO IN SYNC.\n")

    def has_trade_opened_today(self):
        # Always use New York time for SPX trading dates
        ny_tz = pytz.timezone('America/New_York')
        today_ny = datetime.datetime.now(ny_tz).strftime('%Y-%m-%d')

        # 1. Check in-memory flag
        if self.last_trade_date == today_ny:
            return True

        # 2. Check active spreads dictionary
        for meta in self.active_spreads.values():
            entry_time_str = meta.get('entry_time')
            if entry_time_str:
                entry_dt_ny = datetime.datetime.fromisoformat(entry_time_str).astimezone(ny_tz)
                if entry_dt_ny.strftime('%Y-%m-%d') == today_ny:
                    self.last_trade_date = today_ny
                    return True

        # 3. Check Supabase transaction logs (Survives script restarts)
        if self.supabase:
            try:
                start_of_day_ny = datetime.datetime.now(ny_tz).replace(hour=0, minute=0, second=0, microsecond=0)
                start_of_day_utc = start_of_day_ny.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
                res = self.supabase.table('spx_transaction_logs') \
                    .select('id') \
                    .eq('action', 'OPEN_PUT_SPREAD') \
                    .gte('timestamp', start_of_day_utc) \
                    .execute()
                if res.data and len(res.data) > 0:
                    self.last_trade_date = today_ny
                    self._save_state()
                    return True
            except Exception as e:
                print(f"⚠️ DAILY TRADE CHECK DB ERROR: {e}")

        return False

    def _check_week_rotation(self):
        current_iso_week = datetime.date.today().isocalendar()[1]
        if self.last_week_number != current_iso_week:
            if self.last_week_number != 0:
                self.vix_week_mode = 2 if self.vix_week_mode == 1 else 1
            self.last_week_number = current_iso_week
            self._save_state()
            print(f"🔄 WEEK ROTATION: Advanced to VIX Week Mode {self.vix_week_mode}")

    def trigger_signal_cooldown(self):
        self.last_signal_time = datetime.datetime.now(pytz.utc).isoformat()
        self._save_state()

    def is_cooling_off(self):
        if not self.last_signal_time:
            return False
        last_sig = datetime.datetime.fromisoformat(self.last_signal_time)
        cooldown_elapsed = (datetime.datetime.now(pytz.utc) - last_sig).total_seconds()
        return cooldown_elapsed < 600

    def send_email_notification(self, subject, body):
        if not self.gmail_user or not self.gmail_app_password:
            return
        try:
            msg = EmailMessage()
            msg.set_content(body)
            msg['Subject'] = subject
            msg['From'] = self.gmail_user
            msg['To'] = self.notify_email

            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            server.login(self.gmail_user, self.gmail_app_password)
            server.send_message(msg)
            server.quit()
        except Exception as e:
            print(f"⚠️ EMAIL FAILED: {e}")

    def log_transaction(self, trade_type, expiration, short_strike, long_strike, quantity, action, status, price,
                        duration="", details=""):
        timestamp = datetime.datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
        if self.supabase:
            try:
                data = {
                    'timestamp': timestamp,
                    'trade_type': trade_type,
                    'expiration': str(expiration),
                    'short_strike': float(short_strike),
                    'long_strike': float(long_strike),
                    'quantity': int(quantity),
                    'action': action,
                    'status': status,
                    'fill_price': float(price),
                    'duration': duration,
                    'details': details
                }
                self.supabase.table('spx_transaction_logs').insert(data).execute()
            except Exception as e:
                print(f"⚠️ DB LOG ERROR: {e}")
        print(f"📢 LOGGED: {action} {quantity}x {short_strike}P/{long_strike}P @ ~${price:.2f} | Status: {status}")

    def is_regular_trading_hours(self):
        ny_tz = pytz.timezone('America/New_York')
        now_ny = datetime.datetime.now(ny_tz)
        if now_ny.weekday() >= 5:
            return False
        current_time = now_ny.time()
        market_open = datetime.time(9, 30, 0)
        market_close = datetime.time(15, 58, 0)
        return market_open <= current_time <= market_close

    def print_portfolio_status(self):
        """Prints a visual dashboard of open SPX positions and live PnL to the console."""
        if not self.active_spreads:
            print("\n💼 [PORTFOLIO] No active SPX spreads currently held.")
            return

        print("\n" + "=" * 60)
        print("💼 ACTIVE SPX SPREAD HOLDINGS")
        print("=" * 60)

        total_unrealized_pnl = 0.0
        positions = self.ib.positions()
        spx_options = [p for p in positions if
                       p.contract.symbol == 'SPX' and p.contract.secType == 'OPT' and p.position != 0]

        for dict_key, spread_meta in self.active_spreads.items():
            parts = dict_key.split('_')
            if len(parts) != 3:
                continue

            ib_exp, target_short, target_long = parts[0], float(parts[1]), float(parts[2])

            short_legs = [p for p in spx_options if
                          p.contract.lastTradeDateOrContractMonth == ib_exp and p.contract.strike == target_short and p.position < 0]
            long_legs = [p for p in spx_options if
                         p.contract.lastTradeDateOrContractMonth == ib_exp and p.contract.strike == target_long and p.position > 0]

            if not short_legs or not long_legs:
                print(f"⚠️ {ib_exp} | {target_short}P/{target_long}P tracking in DB but legs missing in IBKR.")
                continue

            quantity = abs(short_legs[0].position)
            tc = short_legs[0].contract.tradingClass
            entry_credit = spread_meta.get('entry_credit', 0.0)

            short_put = Option('SPX', ib_exp, target_short, 'P', 'SMART', tradingClass=tc)
            long_put = Option('SPX', ib_exp, target_long, 'P', 'SMART', tradingClass=tc)
            qualified = self.ib.qualifyContracts(short_put, long_put)

            current_value = self.get_live_spread_value(qualified[0], qualified[1]) if len(qualified) == 2 else None

            if current_value is not None and entry_credit > 0:
                unrealized = (entry_credit - current_value) * 100 * quantity
                pct = ((entry_credit - current_value) / entry_credit) * 100
                total_unrealized_pnl += unrealized
                marker = "🟢" if unrealized >= 0 else "🔴"

                print(f"{marker} {ib_exp} | {target_short}P / {target_long}P | Qty: {quantity}")
                print(f"   Entry Credit: ${entry_credit:.2f} | Current Mark: ${current_value:.2f}")
                print(f"   Unrealized PnL: ${unrealized:+.2f} ({pct:+.2f}%)")
            else:
                print(f"⚪ {ib_exp} | {target_short}P / {target_long}P | Qty: {quantity}")
                print(f"   Entry Credit: ${entry_credit:.2f} | Current Mark: Awaiting Pricing...")
            print("-" * 60)

        print(f"💰 Total Strategy Unrealized PnL: ${total_unrealized_pnl:+.2f}")
        print("=" * 60 + "\n")

    def get_dynamic_min_vix(self):
        if self.vix_week_mode == 1:
            dynamic_min_vix = self.vix_threshold_low
            print(f"📉 VIX MODE 1 (Flat Week): Minimum required VIX is {dynamic_min_vix}.")
            return dynamic_min_vix

        now_ny = datetime.datetime.now(pytz.timezone('America/New_York'))
        weekday = now_ny.weekday()
        end_of_week = 4

        if weekday < self.vix_switch_day:
            dynamic_min_vix = self.vix_threshold_high
        elif self.vix_switch_day >= end_of_week:
            dynamic_min_vix = self.vix_threshold_low
        else:
            total_decay_steps = end_of_week - self.vix_switch_day + 1
            current_step = weekday - self.vix_switch_day + 1
            step_fraction = current_step / total_decay_steps
            vix_range = self.vix_threshold_high - self.vix_threshold_low
            dynamic_min_vix = self.vix_threshold_high - (step_fraction * vix_range)

        dynamic_min_vix = round(max(self.vix_threshold_low, dynamic_min_vix), 2)
        print(f"📉 VIX MODE 2 (Decay Week): Threshold current at {dynamic_min_vix}.")
        return dynamic_min_vix

    def calculate_position_size(self, spread_width):
        account_summary = self.ib.accountSummary()

        # 1. Fetch live IBKR Buying Power metrics and their Base Currency
        avail_item = next((i for i in account_summary if i.tag == 'AvailableFunds'), None)
        excess_item = next((i for i in account_summary if i.tag == 'ExcessLiquidity'), None)

        available_funds = float(avail_item.value) if avail_item else 0.0
        excess_liq = float(excess_item.value) if excess_item else 0.0
        base_currency = avail_item.currency if avail_item else 'USD'

        if available_funds <= 0:
            print("🛑 $0 Available Funds. Account is maxed out. Cannot open new positions.")
            return 0

        # 2. Standardize to USD if Account Base Currency is not USD
        if base_currency != 'USD':
            print(f"💱 Converting Base Currency ({base_currency}) to USD for SPX margin math...")

            # Create a Forex contract to get the live exchange rate (e.g., USD/ILS)
            fx_contract = Contract(symbol='USD', secType='CASH', currency=base_currency, exchange='IDEALPRO')
            self.ib.qualifyContracts(fx_contract)

            ticker = self.ib.reqMktData(fx_contract, '', snapshot=True)
            self.ib.sleep(2)

            rate = ticker.marketPrice()
            if math.isnan(rate) or rate <= 0:
                rate = ticker.close if (ticker.close and not math.isnan(ticker.close)) else 0.0

            if rate > 0:
                print(f"   ↳ Live Rate: 1 USD = {rate:.4f} {base_currency}")
                available_funds_usd = available_funds / rate
                excess_liq_usd = excess_liq / rate
            else:
                print(f"⚠️ ERROR: Could not fetch {base_currency}/USD exchange rate. Halting sizing.")
                return 0
        else:
            available_funds_usd = available_funds
            excess_liq_usd = excess_liq

        # 3. Strategy Budget (Now safely in USD)
        strategy_available_funds = available_funds_usd * self.max_account_risk_pct

        # 4. Trade Amount Budget (Now safely in USD)
        actual_trade_budget = strategy_available_funds * self.max_risk_per_trade_pct

        # Margin required for 1 SPX spread contract (Width * 100)
        max_loss_per_new_contract = spread_width * 100.0

        # --- DEBUG PRINTS ---
        print(f"💼 [BUYING POWER & RISK CHECK]")
        print(f"   ├─ Base Currency (IBKR): {available_funds:,.2f} {base_currency}")
        print(f"   ├─ Converted to USD: ${available_funds_usd:,.2f}")
        print(f"   ├─ Excess Liquidity (USD): ${excess_liq_usd:,.2f}")
        print(
            f"   ├─ Available Funds for Strategy ({self.max_account_risk_pct * 100}% of Avail): ${strategy_available_funds:,.2f}")
        print(
            f"   ├─ Trade Amount Budget ({self.max_risk_per_trade_pct * 100}% of Strategy): ${actual_trade_budget:,.2f}")
        print(f"   └─ Margin Required Per Contract (Width {spread_width}): ${max_loss_per_new_contract:,.2f}")

        if actual_trade_budget < max_loss_per_new_contract:
            print(
                f"🛑 Trade Amount Budget (${actual_trade_budget:,.2f}) is too small for 1 contract (${max_loss_per_new_contract:,.2f}).")
            return 0

        affordable_contracts = math.floor(actual_trade_budget / max_loss_per_new_contract)

        # Enforce the hard cap on max contracts per trade
        final_quantity = min(affordable_contracts, self.max_contracts_per_trade)

        print(f"✅ Trade Budget Approved: {final_quantity} contracts.")
        return final_quantity

    def get_live_market_conditions(self):
        try:
            spx_idx, vix_idx = Index('SPX', 'CBOE'), Index('VIX', 'CBOE')
            self.ib.qualifyContracts(spx_idx, vix_idx)

            spx_ticker = self.ib.reqMktData(spx_idx, '', snapshot=False)
            vix_ticker = self.ib.reqMktData(vix_idx, '', snapshot=False)

            timeout = 60
            current_spx, current_vix = float('nan'), float('nan')

            while timeout > 0:
                spx_val = spx_ticker.marketPrice()
                if math.isnan(spx_val) or spx_val == 0:
                    spx_val = spx_ticker.last if (spx_ticker.last and spx_ticker.last > 0) else spx_ticker.close

                vix_val = vix_ticker.marketPrice()
                if math.isnan(vix_val) or vix_val == 0:
                    vix_val = vix_ticker.last if (vix_ticker.last and vix_ticker.last > 0) else vix_ticker.close

                if spx_val > 0 and vix_val > 0 and not math.isnan(spx_val) and not math.isnan(vix_val):
                    current_spx = spx_val
                    current_vix = vix_val
                    break

                self.ib.sleep(0.1)
                timeout -= 1

            self.ib.cancelMktData(spx_idx)
            self.ib.cancelMktData(vix_idx)

            if math.isnan(current_spx) or math.isnan(current_vix) or current_spx == 0 or current_vix == 0:
                print("⚠️ ERROR: Could not fetch valid SPX/VIX prices.")
                print(f"   -> Debug | SPX Last: {spx_ticker.last}, VIX Last: {vix_ticker.last}")
                print("   -> Fix: Ensure you have the 'Cboe Real-Time Index Data' subscription in IBKR.")
                return None, None

            print(f"📊 IBKR LIVE SCAN | SPX: {current_spx:.2f} | VIX: {current_vix:.2f}")
            if self.get_dynamic_min_vix() <= current_vix <= self.max_vix:
                return current_spx, current_vix

            print(f"📢 VOLATILITY FILTER: VIX at {current_vix:.2f} is outside dynamic limits.")
            return None, None

        except Exception as e:
            print(f"⚠️ MARKET DATA ERROR: {e}")
            return None, None

    def get_ibkr_options_chain(self):
        spx = Index('SPX', 'CBOE')
        self.ib.qualifyContracts(spx)
        chains = self.ib.reqSecDefOptParams(spx.symbol, '', spx.secType, spx.conId)

        for chain in chains:
            if chain.tradingClass == 'SPXW' and chain.exchange in ['SMART', 'CBOE']:
                return chain

        for chain in chains:
            if chain.tradingClass == 'SPX' and chain.exchange in ['SMART', 'CBOE']:
                return chain

        return None

    def find_target_expiration(self, chain):
        if not chain:
            print("⚠️ ERROR: No SPX options chain found by IBKR.")
            return None

        ny_tz = pytz.timezone('America/New_York')
        today = datetime.datetime.now(ny_tz).date()

        active_exps = {k.split('_')[0] for k in self.active_spreads.keys()}

        valid_exps = []
        for exp in chain.expirations:
            exp_date = datetime.datetime.strptime(exp, '%Y%m%d').date()
            dte = (exp_date - today).days

            if 30 <= dte <= 45:
                if exp in active_exps:
                    continue

                is_friday = (exp_date.weekday() == 4)
                valid_exps.append((exp, dte, is_friday))

        if valid_exps:
            fridays = [x for x in valid_exps if x[2]]
            if fridays:
                chosen_exp = min(fridays, key=lambda x: x[1])[0]
                print(f"📅 Target Expiration Found: {chosen_exp} (High-Liquidity Friday)")
                return chosen_exp

            chosen_exp = min(valid_exps, key=lambda x: x[1])[0]
            print(f"📅 Target Expiration Found: {chosen_exp} (Standard Day)")
            return chosen_exp

        else:
            print(f"⚠️ ALERT: No new valid expirations found in the 30-45 DTE window (or they are already active).")
            return None

    def get_live_spread_value(self, short_put, long_put, strict_liquidity=False):
        try:
            short_ticker = self.ib.reqMktData(short_put, '101,106', snapshot=False)
            long_ticker = self.ib.reqMktData(long_put, '101,106', snapshot=False)

            timeout = 100
            spread_value = None

            while timeout > 0:
                s_bid, s_ask = short_ticker.bid, short_ticker.ask
                l_bid, l_ask = long_ticker.bid, long_ticker.ask

                # --- 1. LIVE MARKET HOURS LOGIC ---
                if not (math.isnan(s_bid) or math.isnan(s_ask) or math.isnan(l_bid) or math.isnan(l_ask)):
                    if s_bid > 0 and s_ask > 0 and l_bid > 0 and l_ask > 0:

                        if strict_liquidity:
                            s_bid_sz, s_ask_sz = short_ticker.bidSize, short_ticker.askSize
                            l_bid_sz, l_ask_sz = long_ticker.bidSize, long_ticker.askSize
                            s_vol = short_ticker.volume if not math.isnan(short_ticker.volume) else 0
                            l_vol = long_ticker.volume if not math.isnan(long_ticker.volume) else 0

                            if (s_bid_sz < self.min_leg_size or s_ask_sz < self.min_leg_size or
                                    l_bid_sz < self.min_leg_size or l_ask_sz < self.min_leg_size):
                                self.ib.sleep(0.1)
                                timeout -= 1
                                continue

                            if (s_ask - s_bid > self.max_leg_bid_ask_spread) or (
                                    l_ask - l_bid > self.max_leg_bid_ask_spread):
                                break

                            if self.min_daily_volume > 0 and (
                                    s_vol < self.min_daily_volume or l_vol < self.min_daily_volume):
                                break

                        short_mid = (s_bid + s_ask) / 2.0
                        long_mid = (l_bid + l_ask) / 2.0
                        spread_value = short_mid - long_mid
                        break

                # --- 2. FALLBACK TO CLOSE / MARK PRICE ---
                s_close = short_ticker.close if (
                        not math.isnan(short_ticker.close) and short_ticker.close > 0) else short_ticker.last
                l_close = long_ticker.close if (
                        not math.isnan(long_ticker.close) and long_ticker.close > 0) else long_ticker.last

                s_mark = short_ticker.marketPrice()
                l_mark = long_ticker.marketPrice()

                s_price = s_mark if not math.isnan(s_mark) else s_close
                l_price = l_mark if not math.isnan(l_mark) else l_close

                if not strict_liquidity and not math.isnan(s_price) and s_price > 0 and not math.isnan(
                        l_price) and l_price > 0:
                    spread_value = s_price - l_price
                    if timeout < 50:
                        break

                self.ib.sleep(0.1)
                timeout -= 1

            self.ib.cancelMktData(short_put)
            self.ib.cancelMktData(long_put)
            return spread_value

        except Exception as e:
            print(f"⚠️ PRICING ERROR: {e}")
            return None

    def calculate_setup(self, current_spx_price, current_vix, ib_expiration, chain):

        # 1. SPREAD WIDTH LEVERAGE
        # Widening the spread reduces the cost of the long leg, letting you keep more premium.
        # As VIX rises, we widen the net to capture massive premium spikes.
        if current_vix < 15.0:
            spread_width = 30
        elif current_vix < 18.0:
            spread_width = 50
        elif current_vix < 25.0:
            spread_width = 75
        else:
            spread_width = 100

        # 2. VIX-ADJUSTED SAFETY MARGIN (OTM %)
        # Low VIX = lower daily moves, we can step closer.
        # High VIX = violent swings, we must move much further away.
        if current_vix < 15.0:
            target_drop_pct = 0.035  # ~3.5% out of the money
        elif current_vix < 18.0:
            target_drop_pct = 0.05  # ~5.0% out of the money
        elif current_vix < 22.0:
            target_drop_pct = 0.07  # ~7.0% out of the money
        else:
            target_drop_pct = 0.09  # ~9.0% out of the money (Extreme safety)

        fallback_target = current_spx_price * (1.0 - target_drop_pct)

        all_strikes = sorted(list(chain.strikes))
        liquid_strikes = [s for s in all_strikes if s % 25 == 0]
        if not liquid_strikes:
            liquid_strikes = [s for s in all_strikes if s % 10 == 0]
        if not liquid_strikes:
            liquid_strikes = all_strikes

        valid_strikes = sorted(liquid_strikes)
        ideal_strike = min(valid_strikes, key=lambda x: abs(x - fallback_target))
        start_idx = valid_strikes.index(ideal_strike)

        for offset in range(20):
            step = (offset + 1) // 2
            test_idx = start_idx + ((-1 if offset % 2 == 0 else 1) * step)

            if test_idx < 0 or test_idx >= len(valid_strikes):
                continue

            short_strike = valid_strikes[test_idx]
            long_strike = short_strike - spread_width

            if long_strike not in all_strikes:
                continue

            dict_key = f"{ib_expiration}_{float(short_strike)}_{float(long_strike)}"
            if dict_key in self.active_spreads:
                print(f"⚠️ Skipping {short_strike}P/{long_strike}P because it is already active.")
                continue

            tc = chain.tradingClass
            short_put = Option('SPX', ib_expiration, short_strike, 'P', 'SMART', tradingClass=tc)
            long_put = Option('SPX', ib_expiration, long_strike, 'P', 'SMART', tradingClass=tc)

            qualified = self.ib.qualifyContracts(short_put, long_put)

            if len(qualified) == 2:
                estimated_credit = self.get_live_spread_value(qualified[0], qualified[1], strict_liquidity=True)

                if estimated_credit is not None and estimated_credit > 0:

                    # 3. DYNAMIC MINIMUM CREDIT YIELD
                    # Demand high yields when VIX is high, accept lower standard yields when VIX is crushed.
                    if current_vix < 15.0:
                        dynamic_ratio = 0.055  # 5.5% yield
                    elif current_vix < 18.0:
                        dynamic_ratio = 0.07  # 7.0% yield
                    elif current_vix < 25.0:
                        dynamic_ratio = 0.09  # 9.0% yield
                    else:
                        dynamic_ratio = 0.11  # 11.0% yield

                    min_required_credit = spread_width * dynamic_ratio

                    if estimated_credit >= min_required_credit:
                        print(
                            f"🎯 VALID STRIKE PAIR FOUND: {short_strike}/{long_strike} (Est Credit: ${estimated_credit:.2f} >= Min: ${min_required_credit:.2f})")
                        return ib_expiration, float(short_strike), float(long_strike), float(
                            estimated_credit), spread_width, tc
                    else:
                        print(
                            f"⏭️ Skipping {short_strike}/{long_strike}: Credit ${estimated_credit:.2f} is too low (Requires min ${min_required_credit:.2f})")

        print("⚠️ No valid strikes found meeting criteria.")
        return None, None, None, 0.0, spread_width, None

    def create_spread_contract(self, ib_expiration, short_strike, long_strike, tc):
        short_put = Option('SPX', ib_expiration, short_strike, 'P', 'SMART', tradingClass=tc)
        long_put = Option('SPX', ib_expiration, long_strike, 'P', 'SMART', tradingClass=tc)
        qualified = self.ib.qualifyContracts(short_put, long_put)
        if len(qualified) != 2:
            return None

        spread_contract = Contract(symbol='SPX', secType='BAG', currency='USD', exchange='SMART')
        spread_contract.comboLegs = [
            ComboLeg(conId=qualified[0].conId, ratio=1, action='BUY', exchange='SMART'),
            ComboLeg(conId=qualified[1].conId, ratio=1, action='SELL', exchange='SMART')
        ]
        return spread_contract

    def execute_adaptive_entry(self, ib_expiration, short_strike, long_strike, estimated_credit, spread_width,
                               current_vix, tc):
        quantity = self.calculate_position_size(spread_width)
        if quantity < 1:
            print("⚠️ Insufficient account risk capital to place trade.")
            return

        spread_contract = self.create_spread_contract(ib_expiration, short_strike, long_strike, tc)
        if not spread_contract:
            return

        def round_to_05(val):
            return round(val * 20) / 20.0

        current_lmt_price = round_to_05(estimated_credit)
        floor_price = round_to_05(estimated_credit * self.entry_floor_pct)

        print(
            f"🚀 SUBMITTING ENTRY: {quantity}x {short_strike}/{long_strike} | Start LMT: ${current_lmt_price:.2f} | Floor: ${floor_price:.2f}")
        order = LimitOrder('SELL', quantity, current_lmt_price, tif='DAY', outsideRth=False)
        trade = self.ib.placeOrder(spread_contract, order)

        def on_trade_filled(trade_obj, fill_obj):
            if trade_obj.orderStatus.status == 'Filled':
                dict_key = f"{ib_expiration}_{short_strike}_{long_strike}"
                fill_price = trade_obj.orderStatus.avgFillPrice

                self.active_spreads[dict_key] = {
                    'entry_credit': fill_price,
                    'target_profit_pct': 0.70,
                    'entry_time': datetime.datetime.now(pytz.utc).isoformat()
                }

                ny_tz = pytz.timezone('America/New_York')
                self.last_trade_date = datetime.datetime.now(ny_tz).strftime('%Y-%m-%d')
                self.trigger_signal_cooldown()

                self.log_transaction(
                    'ENTRY', ib_expiration, short_strike, long_strike, quantity,
                    'OPEN_PUT_SPREAD', 'Filled', fill_price, "", f"VIX: {current_vix:.2f}"
                )
                self.send_email_notification(
                    subject=f"SPX ENTRY FILLED: {quantity}x {short_strike}/{long_strike}",
                    body=f"Fill Price: ${fill_price:.2f} | Standard Target Profit: 70%"
                )

        trade.fillEvent += on_trade_filled

        for step in range(1, self.entry_retries + 1):
            self.ib.sleep(self.entry_retry_delay_sec)

            if trade.orderStatus.status == 'Cancelled':
                print(f"⚠️ ENTRY CANCELED BY BROKER! Check IBKR margin / trading permission logs.")
                break

            if trade.orderStatus.status == 'Filled':
                break

            new_lmt_price = round_to_05(current_lmt_price - 0.05)

            if new_lmt_price >= floor_price:
                current_lmt_price = new_lmt_price
                order.lmtPrice = current_lmt_price
                print(f"🔄 [Try {step}/{self.entry_retries}] Step down LMT -> ${current_lmt_price:.2f}")
                self.ib.placeOrder(spread_contract, order)
            else:
                print(
                    f"⏳ [Try {step}/{self.entry_retries}] LMT at Floor Price (${floor_price:.2f}). Waiting for fill...")

        if trade.orderStatus.status not in ['Filled', 'Cancelled']:
            self.ib.cancelOrder(order)
            print("⏳ Entry timed out without filling. Order canceled to prevent bad fills.")

    def manage_active_positions(self):
        positions = self.ib.positions()
        spx_options = [p for p in positions if
                       p.contract.symbol == 'SPX' and p.contract.secType == 'OPT' and p.position != 0]

        if not spx_options or not self.active_spreads:
            return

        now_ny = datetime.datetime.now(pytz.timezone('America/New_York'))
        is_friday_close = (now_ny.weekday() == 4 and now_ny.time() >= datetime.time(15, 30, 0))
        today_date = datetime.date.today()

        for dict_key, spread_meta in list(self.active_spreads.items()):
            parts = dict_key.split('_')
            if len(parts) != 3:
                continue

            ib_exp, target_short, target_long = parts[0], float(parts[1]), float(parts[2])

            short_legs = [p for p in spx_options if
                          p.contract.lastTradeDateOrContractMonth == ib_exp and p.contract.strike == target_short and p.position < 0]
            long_legs = [p for p in spx_options if
                         p.contract.lastTradeDateOrContractMonth == ib_exp and p.contract.strike == target_long and p.position > 0]

            if not short_legs or not long_legs:
                continue

            quantity = abs(short_legs[0].position)
            tc = short_legs[0].contract.tradingClass

            short_put = Option('SPX', ib_exp, target_short, 'P', 'SMART', tradingClass=tc)
            long_put = Option('SPX', ib_exp, target_long, 'P', 'SMART', tradingClass=tc)
            qualified = self.ib.qualifyContracts(short_put, long_put)
            if len(qualified) != 2:
                continue

            current_estimated_value = self.get_live_spread_value(qualified[0], qualified[1])
            if current_estimated_value is None:
                continue

            entry_credit = spread_meta.get('entry_credit', abs(short_legs[0].avgCost - long_legs[0].avgCost) / 100.0)
            target_profit_pct = spread_meta.get('target_profit_pct', 0.70)

            total_pnl = (entry_credit - current_estimated_value) * 100.0 * quantity
            pnl_pct = ((entry_credit - current_estimated_value) / entry_credit) * 100 if entry_credit > 0 else 0.0

            print(
                f"💰 LIVE PnL [{dict_key}]: Entry ${entry_credit:.2f} | Current Mark ${current_estimated_value:.2f} | Net: ${total_pnl:.2f} ({pnl_pct:.1f}%)")

            dte = (datetime.datetime.strptime(ib_exp, '%Y%m%d').date() - today_date).days
            exit_reason = None

            entry_time_str = spread_meta.get('entry_time')
            hours_held = 999.0
            if entry_time_str:
                entry_time = datetime.datetime.fromisoformat(entry_time_str)
                hours_held = (datetime.datetime.now(pytz.utc) - entry_time).total_seconds() / 3600.0

            # --- EXIT CONDITION CHECKS ---

            if current_estimated_value <= (entry_credit * (1.0 - target_profit_pct)):
                exit_reason = f"TAKE_PROFIT_{int(target_profit_pct * 100)}PCT"
                self.stop_loss_strikes[dict_key] = 0  # Reset strikes on profit

            elif hours_held <= self.fast_profit_hours and current_estimated_value <= (entry_credit * 0.50):
                exit_reason = "FAST_PROFIT_50PCT"
                self.stop_loss_strikes[dict_key] = 0  # Reset strikes on profit

            elif current_estimated_value >= (entry_credit * 3.0):
                # Ghost Mark Protection: Require 3 consecutive bad ticks before stopping out
                self.stop_loss_strikes[dict_key] = self.stop_loss_strikes.get(dict_key, 0) + 1

                if self.stop_loss_strikes[dict_key] >= 3:
                    exit_reason = "STOP_LOSS_200PCT"
                else:
                    print(
                        f"⚠️️ STOP LOSS WARNING [{dict_key}]: Mark at ${current_estimated_value:.2f}. Strike {self.stop_loss_strikes[dict_key]}/3. Waiting to confirm...")

            elif is_friday_close and dte <= 3:
                exit_reason = "WEEKEND_EXPIRATION_THREAT"
            elif dte <= 7:
                exit_reason = "EXPIRATION_THREAT"
            else:
                # If the price recovers back to normal, reset the strike counter
                if dict_key in self.stop_loss_strikes and self.stop_loss_strikes[dict_key] > 0:
                    print(f"✅ Ghost mark subsided for [{dict_key}]. Resetting stop-loss strikes.")
                    self.stop_loss_strikes[dict_key] = 0

            # -------------------------------------------------------------
            # THE FIX: ACTUALLY EXECUTE THE EXIT
            # -------------------------------------------------------------
            if exit_reason:
                print(f"\n🎯 EXIT CONDITION MET [{dict_key}]: {exit_reason}")
                print(f"   Initiating close sequence for {quantity}x {target_short}P/{target_long}P...")

                # Call the execution function to route the order to IBKR
                self.close_adaptive_spread(
                    ib_expiration=ib_exp,
                    short_strike=target_short,
                    long_strike=target_long,
                    quantity=quantity,
                    estimated_value=current_estimated_value,
                    reason=exit_reason,
                    tc=tc
                )

    def close_adaptive_spread(self, ib_expiration, short_strike, long_strike, quantity, estimated_value, reason, tc):
        spread_contract = self.create_spread_contract(ib_expiration, short_strike, long_strike, tc)
        if not spread_contract:
            return

        def round_to_05(val):
            return round(val * 20) / 20.0

        current_lmt_price = round_to_05(max(0.05, estimated_value))
        ceiling_price = round_to_05(current_lmt_price + 0.30)

        print(f"🚀 SUBMITTING EXIT: {quantity}x {short_strike}/{long_strike} | Target LMT: ${current_lmt_price:.2f}")
        order = LimitOrder('BUY', quantity, current_lmt_price, tif='DAY', outsideRth=False)
        trade = self.ib.placeOrder(spread_contract, order)

        def on_exit_filled(trade_obj, fill_obj):
            if trade_obj.orderStatus.status == 'Filled':
                fill_price = trade_obj.orderStatus.avgFillPrice
                dict_key = f"{ib_expiration}_{short_strike}_{long_strike}"

                entry_time_str = self.active_spreads.get(dict_key, {}).get('entry_time')
                duration_str = ""
                if entry_time_str:
                    entry_time = datetime.datetime.fromisoformat(entry_time_str)
                    duration_str = str(datetime.datetime.now(pytz.utc) - entry_time).split('.')[0]

                self.log_transaction(
                    'EXIT', ib_expiration, short_strike, long_strike, quantity,
                    'CLOSE_PUT_SPREAD', 'Filled', fill_price, duration_str, reason
                )

                if dict_key in self.active_spreads:
                    del self.active_spreads[dict_key]

                self.trigger_signal_cooldown()
                self._save_state()

                self.send_email_notification(
                    subject=f"SPX EXIT FILLED [{reason}]",
                    body=f"Closed SPX {short_strike}/{long_strike}.\nFill Price: ${fill_price:.2f}\nDuration: {duration_str}"
                )

        trade.fillEvent += on_exit_filled

        for step in range(1, self.exit_retries + 1):
            self.ib.sleep(self.exit_retry_delay_sec)

            if trade.orderStatus.status == 'Cancelled':
                print(f"⚠️ EXIT CANCELED BY BROKER! (Check IBKR logs)")
                break

            if trade.orderStatus.status == 'Filled':
                break

            new_lmt_price = round_to_05(min(ceiling_price, current_lmt_price + 0.05))
            if new_lmt_price > current_lmt_price:
                current_lmt_price = new_lmt_price
                order.lmtPrice = current_lmt_price
                print(f"🔄 [Exit Try {step}/{self.exit_retries}] Step up Exit LMT -> ${current_lmt_price:.2f}")
                self.ib.placeOrder(spread_contract, order)

        if trade.orderStatus.status not in ['Filled', 'Cancelled']:
            self.ib.cancelOrder(order)
            print("⏳ Exit timed out without filling. Order canceled. Will retry next loop.")


if __name__ == '__main__':
    print("📢 STARTING FULL LIVE SPX SWING ENGINE (WITH HIGHLANDER LOCK)...")

    safe_client_id = random.randint(1, 9999)
    ib_port = 7496

    trader = TrueLiveSPXSwingTrader(
        ib_port=ib_port,
        client_id=safe_client_id,
        max_account_risk_pct=0.5,
        max_risk_per_trade_pct=0.12,
        max_trades_per_week=2,
        max_total_active_trades=8,
        min_credit_ratio=0.08,
        vix_threshold_high=14.0,
        vix_threshold_low=14.0,
        vix_switch_day=3,
        fast_profit_hours=24.0,
        gmail_user='amitwitt@gmail.com',
        gmail_pass_file='c:/amit/spxpass.txt',
        notify_email='amitwittnotify@gmail.com',
        creds_file='c:/amit/supabase.txt',
        entry_retries=15,
        entry_retry_delay_sec=4.0,
        entry_floor_pct=0.8,
        exit_retries=10,
        exit_retry_delay_sec=5.0
    )


    def responsive_sleep(seconds, heartbeat_msg=None, heartbeat_interval=60):
        """
        Sleeps for 'seconds' in 1-second chunks, maintaining the IB connection and lock.
        If heartbeat_msg is provided, prints a status update every 'heartbeat_interval' seconds.
        """
        for i in range(1, int(seconds) + 1):
            trader.check_execution_lock()
            if not trader.ib.isConnected():
                raise ConnectionError("Socket disconnected during sleep phase.")

            if heartbeat_msg and i % heartbeat_interval == 0:
                current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"💓 [{current_time}] STATUS: {heartbeat_msg} (Sleeping: {i}/{int(seconds)}s)")

            trader.ib.sleep(1)


    while True:
        try:
            trader.check_execution_lock()

            if not trader.ib.isConnected():
                print("🔄 Attempting to reconnect to IBKR...")
                try:
                    trader.connect_ib()
                except Exception as conn_err:
                    print(f"⚠️ Reconnection attempt failed: {conn_err}")
                    time.sleep(15)
                    continue

            # --- MARKET CLOSED LOGIC WITH DETAILED SPREAD PRINT ---
            if not trader.is_regular_trading_hours():
                current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                active_trades = len(trader.active_spreads)

                print(f"\n⏰ [{current_time}] Market is closed. Active Spreads: {active_trades}")

                if active_trades > 0:
                    trader.print_portfolio_status()

                print("   Initiating 15-minute sleep cycle...")

                responsive_sleep(
                    900,
                    heartbeat_msg=f"Market Closed | Tracking {active_trades} active positions",
                    heartbeat_interval=60
                )
                continue
            # -----------------------------------------------------

            if trader.is_cooling_off():
                print("⏳ System is in cooldown mode. Sleeping for 60 seconds...")
                responsive_sleep(60)
                continue

            print(f"\n--- INITIATING MARKET SCAN: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

            trader._check_week_rotation()
            trader.reconcile_portfolio_state()
            trader.manage_active_positions()
            trader.print_portfolio_status()

            if trader.has_trade_opened_today():
                print("✅ A trade was already opened today (NY Time). Skipping entry scan, monitoring positions...")
                responsive_sleep(60)
                continue

            # 1. Total Active Spreads Capacity Limit
            if len(trader.active_spreads) >= trader.max_total_active_trades:
                print(
                    f"🛑 Maximum total active trades ({len(trader.active_spreads)}/{trader.max_total_active_trades}) reached. Skipping entry scan, monitoring positions...")
                responsive_sleep(60)
                continue

            # 2. Weekly Velocity Entry Limit (Active Slots System)
            ny_tz = pytz.timezone('America/New_York')
            current_iso_week = datetime.datetime.now(ny_tz).isocalendar()[1]
            current_year = datetime.datetime.now(ny_tz).year

            current_week_active_slots = 0
            for spread_meta in trader.active_spreads.values():
                entry_time_str = spread_meta.get('entry_time')
                if entry_time_str:
                    entry_time_ny = datetime.datetime.fromisoformat(entry_time_str).astimezone(ny_tz)
                    if entry_time_ny.isocalendar()[1] == current_iso_week and entry_time_ny.year == current_year:
                        current_week_active_slots += 1

            if current_week_active_slots >= trader.max_trades_per_week:
                print(
                    f"🛑 Maximum active slots for this week ({current_week_active_slots}/{trader.max_trades_per_week}) are currently filled. Skipping entry scan...")
                responsive_sleep(60)
                continue

            current_spx, current_vix = trader.get_live_market_conditions()

            if current_spx and current_vix:
                ibkr_chain = trader.get_ibkr_options_chain()
                if ibkr_chain:
                    ib_target_exp = trader.find_target_expiration(ibkr_chain)
                    if ib_target_exp:
                        ib_exp, short_strike, long_strike, est_credit, spread_width, tc = trader.calculate_setup(
                            current_spx, current_vix, ib_target_exp, ibkr_chain
                        )
                        if est_credit > 0 and tc is not None:
                            trader.execute_adaptive_entry(ib_exp, short_strike, long_strike, est_credit, spread_width,
                                                          current_vix, tc)

            print("💤 Scan complete. Sleeping 5 minutes before next cycle...\n")
            responsive_sleep(300)

        except (ConnectionError, asyncio.exceptions.CancelledError, OSError) as e:
            print(f"\n🚨 IBKR CONNECTION LOST: {e}")
            print("⏳ Standard time.sleep() initiated. Retrying connection in 60 seconds...")
            try:
                trader.ib.disconnect()
            except Exception:
                pass
            time.sleep(60)

        except KeyboardInterrupt:
            print("\n📢 SYSTEM ALERT: ENGINE MANUALLY STOPPED BY USER.")
            break

        except Exception as e:
            print(f"\n⚠️ UNEXPECTED ERROR: {e}")
            print("⏳ Pausing for 60 seconds before retrying...")
            time.sleep(60)

    if trader.ib.isConnected():
        trader.ib.disconnect()