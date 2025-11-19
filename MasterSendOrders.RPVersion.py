#!/usr/bin/env python3
import collections, datetime, pytz, queue, re, threading, time, uuid, sys, statistics, threading, collections

import quickfix as fix
import quickfix50sp2 as fix50sp2

from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

rpdir = Path("/home/ec2-user/pythonQF")

def _q(x, quantum="0.01"):
        # Quantize Decimal x to the given quantum string (default 1 cent)
        return Decimal(x).quantize(Decimal(quantum), rounding=ROUND_HALF_UP)

def price_for_symbol(symbol: str, base_px: Optional[float] = None) -> float:
    # Derive a price from the symbol suffix, or just use base_px if given.
    # Example: CBBTC_123125_65000 -> 65000.0
    if base_px is not None:
        return base_px
    try:
        strike = symbol.rsplit("_", 1)[1]
        return float(strike)
    except Exception:
        return 1.0

# Build child paths
data_dir = rpdir / "data"
log_file = rpdir / "logs" / "app.log"
rplog_file = rpdir / "logs" / "rpapp.log"

# -------------------- USER DEFAULTS (edit here) --------------------
# SenderSubID (FIX tag 50): identifies THIS trading application to the venue.
# This rarely changes unless you run multiple logical traders.
SENDER_SUB_ID = "4C001"

# All your products are BUY-only (binary options). Keep True.
SIDE_BUY = True

# -------------------- TRADING USER CONTROL ------------------------
# Which trading account to use (FIX tag 1 = Account).
# Only ONE should be active at a time.
ACCOUNT = "yesRonaldo"
# ACCOUNT = "noRonaldo"
# ACCOUNT = "noTippy"
# ACCOUNT = "yesTippy"
# ACCOUNT = "RPTEST"

# ---------------- MULTI-SYMBOL LADDER (ladder_multi mode) ---------
# These are all the strikes for Dec 31 2025 BTC binary options.
# ladder_multi will cycle through ALL of these.
SYMBOLS = [
    "CBBTC_123125_42500",
    "CBBTC_123125_45000",
    "CBBTC_123125_52500",
    "CBBTC_123125_55000",
    "CBBTC_123125_62500",
    "CBBTC_123125_65000",
    "CBBTC_123125_72500",
    "CBBTC_123125_75000",
    "CBBTC_123125_82500",
    "CBBTC_123125_85000",
    "CBBTC_123125_92500",
    "CBBTC_123125_95000",
    "CBBTC_123125_102500",
    "CBBTC_123125_105000",
    "CBBTC_123125_112500",
    "CBBTC_123125_115000",
    "CBBTC_123125_122500",
    "CBBTC_123125_125000",
    "CBBTC_123125_132500",
    "CBBTC_123125_135000",
    "CBBTC_123125_142500",
    "CBBTC_123125_145000",
    "CBBTC_123125_152500",
    "CBBTC_123125_155000",
    "CBBTC_123125_162500",
    "CBBTC_123125_165000",
    "CBBTC_123125_175000",
]

# -------- SINGLE-SYMBOL CONTROL (all modes *except* ladder_multi) --
# Modes: simplerepeat, layer, replace, ratchet, cancel
# Only one of these should be selected at a time.
# TIP: SYMBOLS[0] is just a safe default; override as needed.
SYMBOL = SYMBOLS[0]
SYMBOL = "CBBTC_123125_162500"
# SYMBOL = "CBBTC_123125_165000"
# SYMBOL = "CBBTC_123125_175000"
# SYMBOL = "MNYCG_110425_Mamdani"

# ---------------- INSTRUMENT CONTROL ------------------------------
# Optional FIX tag 762 (SecuritySubType). Used by your venue.
SecSubType   = "YES"
# SecSubType = "NO"

# -------------- NUMERIC DEFAULTS (used by MOST modes) --------------
PRICE        = 0.51      # initial working price for new orders
QTY          = 1         # quantity per order

# ------------ MODE: simplerepeat, layer, replace, ratchet ----------
# maxloop = how many orders to send in those modes.
maxloop      = 10

# ---------------- MODE: layer / replace parameters -----------------
# scope defines the TOP of the layer range:
#   layer prices: PRICE → PRICE+step → PRICE+step*2 ... → PRICE+step*N until within scope
scope        = 0.01      # e.g. PRICE=0.51, step=0.01 => range to ~0.52
step         = 0.01      # increment for laddering/replacement

# ---------------- MODE: ratchet parameters -------------------------
ratchet_repeats = 20        # number of cancel/replace cycles per symbol
ratchet_pause_s = 0.0002    # delay between replaces (controls throughput)

# ------------- PRICE QUANTIZATION + SAFETY LIMITS ------------------
# PRICE_QUANTUM: smallest allowable tick size (0.01 = cents)
PRICE_QUANTUM = 0.01

# MAX_PRICE is a safety guard — ratchet logic must NEVER exceed this.
MAX_PRICE = 0.99

# When ladder_multi runs, this holds symbol → current ClOrdID
ladder_state = {}

# ----------------- GLOBAL FIX PARAMETERS ---------------------------
# Time-In-Force (tag 59) for all new orders.
tif  = fix.TimeInForce_DAY
# tif = fix.TimeInForce_GOOD_TILL_CANCEL
# tif = fix.TimeInForce_IMMEDIATE_OR_CANCEL
# tif = fix.TimeInForce_FILL_OR_KILL
# tif = fix.TimeInForce_GOOD_TILL_DATE      # requires tag 432 ExpireDate

# .......the trackedorder variables keep track of all the orders going out so we can replace the value
@dataclass
class TrackedOrder:
    symbol: str
    side: int
    qty: float
    tif: Optional[int] = None
    account: Optional[str] = None
    sec_subtype: Optional[str] = None
    first_clordid: str = ""
    last_clordid: str = ""
    order_id: Optional[str] = None
    price: Optional[float] = None
    live: bool = False
    pending_replace: bool = False

class OrderTracker:
    def __init__(self):
        self._by_last_clordid: Dict[str, TrackedOrder] = {}
        self._lock = threading.Lock()

    def register_new(self, ord: TrackedOrder):
        with self._lock:
            self._by_last_clordid[ord.last_clordid] = ord

    def on_ack_new(self, last_clordid: str, order_id: Optional[str]):
        with self._lock:
            o = self._by_last_clordid.get(last_clordid)
            if o:
                o.order_id = order_id
                o.live = True

    def on_ack_replace(self, orig_clordid: str, new_clordid: str, new_price: Optional[float]):
        with self._lock:
            o = self._by_last_clordid.pop(orig_clordid, None)
            if o:
                o.last_clordid = new_clordid
                if new_price is not None:
                    o.price = new_price
                o.pending_replace = False
                o.live = True
                self._by_last_clordid[new_clordid] = o

    def get_by_last_clordid(self, clordid: str) -> Optional[TrackedOrder]:
        with self._lock:
            return self._by_last_clordid.get(clordid)

    def mark_pending_replace(self, clordid: str):
        with self._lock:
            o = self._by_last_clordid.get(clordid)
            if o:
                o.pending_replace = True
    def mark_pending_cancel(self, cl):
        o = self.get_by_last_clordid(cl)
        if o: o.pending = "cancel"

    def update_on_reject(self, cl, code=None):
        # For 35=9: mark not-live when too-late/unknown (tune codes per venue)
        o = self.get_by_last_clordid(cl)
        if o:
            if code in (1, 6):  # 1=Unknown, 6=Too late (common)
                o.live = False
            o.pending = None
    
    def live_clordids(self):
        # Return the latest ClOrdID for each order chain that is still live (working).
        with self._lock:
            return [
                o.last_clordid
                for o in self._by_last_clordid.values()
                if getattr(o, "live", False)
            ]

class App(fix.Application):
    def __init__(self):
        super().__init__()
        self.session_id = None 
        self.logged_on = False
        self.tracker = OrderTracker()   # <-- for replace
        self._ev = collections.defaultdict(queue.Queue)  # keyed by ClOrdID
        
    def onCreate(self, sid):
        # CRITICAL: Set the session ID here upon creation
        self.session_id = sid 
        print(f"[onCreate] Session created: {sid}")
    
    def onLogon(self, sid):
        # CRITICAL confirmation: Set logged_on to True when the Logon message is acknowledged by the server
        self.logged_on = True
        print(f"[onLogon] Logged on: {sid}")
        print(f"[DEBUG] tracker size={len(self.tracker._by_last_clordid)} live={len(self.tracker.live_clordids())}")
        
    def onLogout(self, sid):
        self.logged_on = False
        print(f"[onLogout] Logged out: {sid}")
        # keep the last session_id so we can re-use on reconnect if needed to hold the logout...
    
    # this function makes the CTRL+C close cleanly because we never call onLogout due to it's requirement with QuickFIX
    def logout_and_stop(self, init, wait_secs=5):
        try:
            if self.session_id:
                sess = fix.Session.lookupSession(self.session_id)
                if sess:
                    sess.logout("Client requested shutdown")
            t0 = time.time()
            while self.logged_on and (time.time() - t0) < wait_secs:
                time.sleep(0.1)
        finally:
            try:
                init.stop()
            except Exception as e:
                print(f"[WARN] init.stop() raised: {e}", file=sys.stderr)

    def toAdmin(self, msg, sid):
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_Logon:
            
            # 1) never send SenderSubID(50) on Logon
            try:
                msg.getHeader().removeField(50)
            except fix.FieldNotFound:
                pass

            # 2) baseline FIXT.1.1 / 50SP2 logon fields
            msg.setField(fix.EncryptMethod(0))        # 98=0 (None)
            msg.setField(fix.HeartBtInt(30))          # 108=30
            msg.setField(fix.DefaultApplVerID("9"))   # 1137=9 (FIX50SP2)
            msg.setField(fix.ResetSeqNumFlag(True))   # 141=Y

    def fromAdmin(self, msg, sid):
        pass
    
    def toApp(self, msg, sid):
        # Add SenderSubID(50) to *all* application messages
        msg.getHeader().setField(fix.SenderSubID(SENDER_SUB_ID))

    #need to add code in fromApp so your app can learn when an order is live and what the broker’s OrderID(37) is.
    #That info only arrives in application messages—specifically ExecutionReport (35=8)—which QuickFIX delivers to fromApp.
    def fromApp(self, msg, sid):
        # keep raw
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(msg.toString() + "\n")

        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() != fix.MsgType_ExecutionReport:  # only 35=8 matters here
            return

        # Parse core fields
        cl11 = fix.ClOrdID();      msg.getField(cl11)
        cl = cl11.getValue()

        et  = fix.ExecType();      msg.getField(et)
        etv = et.getValue()        # string code: '0','5', etc.

        # optional (status & balances)
        stv = None
        try:
            st = fix.OrdStatus(); msg.getField(st); stv = st.getValue()
        except fix.FieldNotFound:
            pass

        order_id = None
        try:
            oid = fix.OrderID(); msg.getField(oid); order_id = oid.getValue()
        except fix.FieldNotFound:
            pass

        if etv == fix.ExecType_NEW:               # 150=0
            self.tracker.on_ack_new(cl, order_id)

        elif etv == fix.ExecType_REPLACE:         # 150=5
            try:
                orig = fix.OrigClOrdID(); msg.getField(orig)
                new_px = None
                try:
                    px = fix.Price(); msg.getField(px); new_px = float(px.getValue())
                except fix.FieldNotFound:
                    pass
                self.tracker.on_ack_replace(orig.getValue(), cl, new_px)
            except fix.FieldNotFound:
                pass

        # notify waiters
        self._ev[cl].put({"exec_type": etv, "ord_status": stv, "clordid": cl})
    
    def wait_for_exec(self, cl, want_exec_types=("0","5"), timeout=0.5):
        end = time.time() + timeout
        q = self._ev[cl]
        while time.time() < end:
            try:
                ev = q.get(timeout=max(0, end - time.time()))
            except queue.Empty:
                return None
            if ev["exec_type"] in want_exec_types:
                return ev
        return None

    def send_limit(self, symbol, buy, qty, price, sec_subtype, account=None, tif=None):
        nos = fix50sp2.NewOrderSingle()
        cl = f"CL-{uuid.uuid4()}"
        tif_val = tif if tif is not None else fix.TimeInForce_DAY

        # FIX fields (always BUY)
        nos.setField(fix.ClOrdID(cl))                           # 11
        if account:     nos.setField(fix.Account(account))      # 1
        nos.setField(fix.Symbol(symbol))                        # 55
        nos.setField(fix.Side(fix.Side_BUY))                    # 54 (constant BUY)
        nos.setField(fix.TransactTime())                        # 60
        nos.setField(fix.OrdType(fix.OrdType_LIMIT))            # 40=2
        nos.setField(fix.OrderQty(float(qty)))                  # 38
        nos.setField(fix.Price(float(price)))                   # 44
        nos.setField(fix.CustOrderCapacity(1))                  # 582
        nos.setField(fix.AccountType(1))                        # 581
        if sec_subtype: nos.setField(fix.SecuritySubType(sec_subtype))  # 762
        nos.setField(fix.TimeInForce(tif_val))                  # 59

        # Track locally before sending (avoid race with fast ExecReports)
        self.tracker.register_new(TrackedOrder(
            symbol=symbol,
            side=fix.Side_BUY,             # fixed constant
            qty=float(qty),
            tif=tif_val,
            account=account,
            sec_subtype=sec_subtype,
            first_clordid=cl,
            last_clordid=cl,
            price=float(price),
            live=False
        ))

        ok = fix.Session.sendToTarget(nos, self.session_id)

        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(f"[SEND] LIMIT {symbol} BUY {qty} @ {price} {sec_subtype} -> {ok}\n")

        return cl
    

    def run_layer_with_maxloop(app, symbol, price, scope, step, qty, account, secsubtype,
                            side_buy=True, price_quantum="0.01", max_orders=1000):
        # max_orders = “upper bound on how many orders this function should emit”
        # Sends up to max_orders orders while bouncing the price between
        # [price .. price+scope] inclusive, stepping by 'step' and reversing at the edges.
        
        low  = _q(str(price),  price_quantum)
        high = _q(str(Decimal(str(price)) + Decimal(str(scope))), price_quantum)
        inc  = _q(str(step),   price_quantum)

        # current price and direction (+1 up, -1 down)
        current    = low
        direction  = Decimal(1)
        orders_sent = 0
        # Optional: 10 trade "types" (uncomment / customize if you want per-tick variety)
        TRADE_TYPES = None
        # Example:
        # TRADE_TYPES = [
        #     {"account":"yesRonaldo","secsub":"YES","qty":1},
        #     {"account":"yesRonaldo","secsub":"NO", "qty":1},
        # ]
        
        while orders_sent < max_orders:
            # send one (or many) order(s) at 'current'
            if TRADE_TYPES:
                for tt in TRADE_TYPES:
                    if orders_sent >= max_orders:
                        break
                    app.send_limit(
                        symbol=symbol,
                        buy=side_buy,
                        qty=tt["qty"],
                        price=float(current),
                        SecSubType=tt["secsub"],
                        account=tt["account"]
                    )
                    orders_sent += 1
            else:
                # single-type send using provided account/secsubtype/qty
                app.send_limit(
                    symbol=symbol,
                    buy=side_buy,
                    qty=qty,
                    price=float(current),
                    SecSubType=secsubtype,
                    account=account
                )
                orders_sent += 1

            # edge checks + bounce
            if current >= high:
                direction = Decimal(-1)
            elif current <= low:
                direction = Decimal(1)

            # step to next price, staying quantized & in-bounds
            current = _q(str(current + direction * inc), price_quantum)
            if current > high:
                current = high
            if current < low:
                current = low

        return orders_sent

    def send_replace(self, last_clordid, *, new_price=None, new_qty=None, ord_type=fix.OrdType_LIMIT,restate_fields=True):
        # 0) lookup + live guard
        o = self.tracker.get_by_last_clordid(last_clordid)
        if not o or not getattr(o, "live", True):
            print(f"[WARN] Cannot replace; order not live or unknown: {last_clordid}")
            return None

        # 1) common builder
        msg, new_cl = self.build_linked_fix(o, fix50sp2.OrderCancelReplaceRequest, "RE")

        # 2) message-specific fields
        msg.setField(fix.OrdType(ord_type))  # 40 required on 35=G
        if new_qty is not None:
            msg.setField(fix.OrderQty(float(new_qty)))  # 38
        if ord_type == fix.OrdType_LIMIT:
            if new_price is not None:
                msg.setField(fix.Price(float(new_price)))  # 44
            elif getattr(o, "price", None) is not None:
                msg.setField(fix.Price(float(o.price)))    # restate unchanged

        # 3) optional restatements (venue-dependent)
        if restate_fields:
            if getattr(o, "tif", None) is not None:
                msg.setField(fix.TimeInForce(o.tif))               # 59
            if getattr(o, "account", None):
                msg.setField(fix.Account(o.account))               # 1
            if getattr(o, "sec_subtype", None):
                msg.setField(fix.SecuritySubType(o.sec_subtype))   # 762

        # 4) tracker transition
        try:
            self.tracker.mark_pending_replace(o.last_clordid)
        except Exception:
            pass

        # 5) send + return new ClOrdID
        fix.Session.sendToTarget(msg, self.session_id)
        return new_cl

    def send_cancel(self, last_clordid, *, restate_fields=True):
        # 0) lookup + live guard
        o = self.tracker.get_by_last_clordid(last_clordid)
        if not o or not getattr(o, "live", True):
            print(f"[WARN] Cannot cancel; order not live or unknown: {last_clordid}")
            return None

        # 1) common builder
        msg, new_cl = self.build_linked_fix(o, fix50sp2.OrderCancelRequest, "CA")

        # 2) (no cancel-specific fields beyond the builder’s linkage + qty)

        # 3) optional restatements (use same flags for symmetry)
        if restate_fields:
            if getattr(o, "tif", None) is not None:
                msg.setField(fix.TimeInForce(o.tif))               # 59
            if getattr(o, "account", None):
                msg.setField(fix.Account(o.account))               # 1
            if getattr(o, "sec_subtype", None):
                msg.setField(fix.SecuritySubType(o.sec_subtype))   # 762

        # 4) tracker transition
        try:
            self.tracker.mark_pending_cancel(o.last_clordid)
        except Exception:
            pass

        # 5) send + return new ClOrdID
        fix.Session.sendToTarget(msg, self.session_id)
        return new_cl    

    def build_linked_fix(self, o, msg_cls, new_prefix):
        # Build a FIX amendment message skeleton linked to an existing tracked order.
        # Build a Cancel or Cancel/Replace from the same template.
        # Common fields: 41 OrigClOrdID 11 new ClOrdID 55 Symbol
        # 54 Side 60 TransactTime optionally 37 OrderID and 38 Qty
        # Returns (msg, new_clordid)
        
        msg = msg_cls()
        new_cl = f"{new_prefix}-{uuid.uuid4()}"

        # Required cross-ref to original
        msg.setField(fix.OrigClOrdID(o.last_clordid))  # 41
        msg.setField(fix.ClOrdID(new_cl))              # 11

        # Common identity fields
        msg.setField(fix.Symbol(o.symbol))             # 55
        msg.setField(fix.Side(o.side))                 # 54
        msg.setField(fix.TransactTime())               # 60

        # Helpful/venue-specific but safe to include
        if getattr(o, "order_id", None):
            msg.setField(fix.OrderID(o.order_id))      # 37
        if getattr(o, "qty", None):
            msg.setField(fix.OrderQty(float(o.qty)))   # 38

        return msg, new_cl    

def build_symbol_ladder(app, symbols, qty, account, secsubtype,
                        side_buy=True, base_px=None):

    # Send one limit order per symbol and remember the last ClOrdID per symbol.
    # Returns: dict {symbol: clordid}

    ladder_state = {}

    for sym in symbols:
        px = price_for_symbol(sym, base_px=base_px)
        cl = app.send_limit(
            symbol=sym,
            buy=side_buy,
            qty=qty,
            price=px,
            sec_subtype=secsubtype,
            account=account,
        )

        print(f"[LADDER] {sym}: sent NEW order {cl} @ {px}")
        ladder_state[sym] = cl

        # optional: wait for NEW ack per order (ExecType=0)
        ev = app.wait_for_exec(cl, want_exec_types=("0",), timeout=1.0)
        if ev is None:
            print(f"[WARN] No NEW ack for {sym} / {cl} within 1s")

    return ladder_state

def ratchet_ladder(app, ladder_state, step, repeats, price_quantum="0.01"):
    # For each symbol in ladder_state, repeatedly replace the order price
    # in a band [PRICE .. (PRICE + step + scope)], then jump back to PRICE.
    # Never exceed MAX_PRICE (for error control).
    # Uses globals: PRICE, scope, MAX_PRICE.    
    base      = Decimal(str(PRICE))        # 0.51
    step_dec  = Decimal(str(step))         # 0.01
    scope_dec = Decimal(str(scope))        # e.g. 0.09
    max_dec   = Decimal(str(MAX_PRICE))    # 0.99

    low  = base
    high = base + step_dec + scope_dec     # e.g. 0.51 + 0.01 + 0.09 = 0.61

    for r in range(repeats):
        print(f"[RATCHET] pass {r+1}/{repeats}")

        for sym, current_cl in list(ladder_state.items()):
            order = app.tracker.get_by_last_clordid(current_cl)
            if order is None:
                print(f"[SKIP] {sym}: no order found for {current_cl}")
                continue

            # Use tracked price or fall back to global PRICE
            old_px = order.price if getattr(order, "price", None) is not None else PRICE
            cur = Decimal(str(old_px))

            # --- PRICE -> PRICE+step -> ... -> HIGH -> PRICE -> ... ---
            if cur >= high:
                new_dec = low
            else:
                new_dec = cur + step_dec
                if new_dec > high:
                    new_dec = high

            # Safety cap
            if new_dec > max_dec:
                new_dec = max_dec

            # Optional: snap to quantum (e.g. cents)
            if price_quantum:
                q = Decimal(price_quantum)
                new_dec = (new_dec / q).quantize(Decimal("1")) * q

            new_px = float(new_dec)

            new_cl = app.send_replace(current_cl, new_price=new_px)
            if not new_cl:
                print(f"[WARN] Replace send failed for {sym} at {new_px}")
                continue

            ladder_state[sym] = new_cl
            print(f"[RATCHET] {sym}: {current_cl} -> {new_cl} @ {new_px:.2f}")

            # Optional: wait for REPLACE ack, but don't fail hard if missing
            ev = app.wait_for_exec(new_cl, want_exec_types=("5",), timeout=1.0)
            if ev is None:
                print(f"[WARN] No REPLACE ack for {sym} / {new_cl} within 1s")

def main(cfg, trademode):
    settings = fix.SessionSettings(cfg)   
    app = App()
    times = [] 
    
    # Initialization MUST be done outside the main try block
    store = fix.FileStoreFactory(settings)
    logs = fix.FileLogFactory(settings)
    init = fix.SocketInitiator(app, store, settings, logs)
    
    init.start()

    logon_timeout_secs = 10 
    start_time = time.time()
    stopped = False
    
    # Want to delete at some point but this is a good check for now...
    try:
        # 1. Wait for session object to be created
        while app.session_id is None and (time.time() - start_time) < 5:
            time.sleep(0.1)

        if app.session_id is None:
            print("ERROR: Failed to create session object. Check config file paths/permissions.")
            return

        # 2. WAIT FOR SUCCESSFUL LOGON (max 10 seconds total)
        while not app.logged_on and (time.time() - start_time) < logon_timeout_secs:
            time.sleep(0.1)

        if not app.logged_on:
            print(f"ERROR: Failed to LOGON within {logon_timeout_secs} seconds. Orders NOT SENT.")
            print("Action: Check your QuickFIX logs for the server's rejection reason (Tags 49, 56, 34, etc.).")
            # If logon fails, we stop here (do not proceed to order loop)
            return 
        
        # --- TRADING LOOP STARTS HERE (Only if logon succeeded) ---
        start_ns = time.perf_counter_ns()
        start_dt = datetime.fromtimestamp(start_ns / 1_000_000_000)
        ms = start_dt.microsecond // 1000
        print(f"Start time : {start_dt.strftime('%H:%M:%S')}.{ms:03d}")
        print("Before the loop here are the values:",
              SYMBOL, SIDE_BUY, QTY, PRICE, SecSubType, ACCOUNT)
        
        # --- main logical main loop start ---
        if trademode == "layer":
            # Use the variables you set at the top:
            low   = _q(PRICE, "0.01")
            high  = _q(Decimal(str(PRICE)) + Decimal(str(scope)), "0.01")
            stepD = _q(step, "0.01")

            orders_sent = 0
            # Keep sending until we hit maxloop (total orders), bouncing between low..high..low
            while orders_sent < maxloop:
                # ---- UP LEG: low -> high (inclusive) ----
                p = low
                while p <= high and orders_sent < maxloop:
                    app.send_limit(SYMBOL, SIDE_BUY, QTY, float(p), SecSubType, ACCOUNT)
                    orders_sent += 1
                    p = _q(p + stepD, "0.01")

                # ---- DOWN LEG: (high - step) -> low (inclusive) ----
                p = _q(high - stepD, "0.01")
                while p >= low and orders_sent < maxloop:
                    app.send_limit(SYMBOL, SIDE_BUY, QTY, float(p), SecSubType, ACCOUNT)
                    orders_sent += 1
                    p = _q(p - stepD, "0.01")

            print(f"[layer] total orders sent: {orders_sent}")

        elif trademode == "simplerepeat":
            i = 1
            NEWPRICE = PRICE
            orders_sent = 0 
            while i <= maxloop:
                start = time.perf_counter()                
                app.send_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)                
                time.sleep(0.0001)  # optional throttle current nano (0.0001)
                i += 1
                orders_sent += 1 

            print(f"[simplerepeat] total orders sent: {orders_sent}")

        elif trademode == "replace":
            created = []
            # 1) send a batch
            for i in range(maxloop):
                cl = app.send_limit(SYMBOL, SIDE_BUY, QTY, PRICE, SecSubType, ACCOUNT)
                created.append(cl)
                time.sleep(0.01)  # gentle pacing

            # 2) brief settle for NEW acks (optional)
            for cl in created:
                app.wait_for_exec(cl, want_exec_types=("0",), timeout=0.5)

            # 3) selective replace (+step) only if order is live and not done
            replaced, skipped = 0, 0
            for cl in created:
                o = app.tracker.get_by_last_clordid(cl)
                if not o or not o.live:
                    skipped += 1
                    continue
                new_px = (o.price if o.price is not None else PRICE) + step
                new_cl = app.send_replace(cl, new_price=new_px)
                if new_cl:
                    replaced += 1

            print(f"[replace] sent={len(created)} replaces={replaced} skipped={skipped}")

        elif trademode == "cancel":  # same shape as your replace demo
            if not app.logged_on:
                print("[ERROR] Not logged on; cannot cancel.")
                return

            # 1) Send N BUY limits (registers in tracker before send)
            created = []
            for i in range(maxloop):
                cl = app.send_limit(
                    SYMBOL,
                    True,               # always BUY
                    QTY,
                    PRICE,  
                    SecSubType,
                    ACCOUNT,
                    tif
                )
                created.append(cl)

            # 2) Wait for NEW acks so tracker flips live=True
            acks = 0
            for cl in created:
                try:
                    app.wait_for_exec(cl, want_exec_types=("0",), timeout=2.0)  # ExecType=NEW
                    acks += 1
                except Exception:
                    pass
            print(f"[INFO] NEW acks received: {acks}/{len(created)}")

            # 3) Cancel exactly those orders (same shape as replace demo)
            sent = []
            for cl in created:
                new_cl = app.send_cancel(cl)
                if new_cl:
                    sent.append(new_cl)
            print(f"[INFO] Sent {len(sent)} cancels (35=F)")

            # 4) (optional) Wait for CANCELED acks
            done = 0
            for cxl_cl in sent:
                try:
                    app.wait_for_exec(cxl_cl, want_exec_types=("4",), timeout=3.0)  # ExecType=CANCELED
                    done += 1
                except Exception:
                    pass
            print(f"[RESULT] Canceled {done}/{len(sent)} within timeout")

        elif trademode == "ratchet":
            # Send one live order, then keep nudging its price and wait for each confirmation (ExecutionReport 150=5) before nudging again
            print("[MODE] ratchet: 1 new order, then replace it repeatedly")

            # 1) Send one NEW order
            base_px = PRICE
            cl = app.send_limit(SYMBOL, SIDE_BUY, QTY, base_px, SecSubType, ACCOUNT)

            # 2) Wait briefly for NEW ack (ExecutionReport 35=8 / 150=0)
            app.wait_for_exec(cl, want_exec_types=("0",), timeout=1.0)

            LOW  = Decimal("0.51")
            HIGH = Decimal("0.61")
            MAX  = Decimal("1.00")
            STEP = Decimal(str(step))

            for k in range(ratchet_repeats):
                o = app.tracker.get_by_last_clordid(current_cl)
                if not o:
                    print(f"[ratchet] stop: order not found (cl={current_cl}) at step {k}")
                    break

                old_px = o.price if getattr(o, "price", None) is not None else PRICE
                cur = Decimal(str(old_px))

                if cur >= HIGH:
                    new_dec = LOW
                else:
                    new_dec = cur + STEP
                    if new_dec > HIGH:
                        new_dec = HIGH

                if new_dec > MAX:
                    new_dec = MAX

                next_px = float(new_dec)

                new_cl = app.send_replace(current_cl, new_price=next_px)
                if not new_cl:
                    print(f"[ratchet] replace send failed at step {k}")
                    break

                ev = app.wait_for_exec(new_cl, want_exec_types=("5",), timeout=1.0)
                if ev is None:
                    print(f"[ratchet] no replace ACK within timeout at step {k} (cl={new_cl})")

                current_cl = new_cl
                time.sleep(ratchet_pause_s)            

            print(f"[ratchet] done: replaces_ok={replaces_ok}/{ratchet_repeats}")
        
        elif trademode == "ladder_multi":
            print("[MODE] ladder_multi: one order per symbol, then ratchet each")

            # 1) build the initial ladder: ALL start at PRICE (0.51), not the strike
            ladder_state = build_symbol_ladder(
                app=app,
                symbols=SYMBOLS,
                qty=QTY,
                account=ACCOUNT,
                secsubtype=SecSubType,
                side_buy=SIDE_BUY,
                base_px=PRICE,     # <<< force initial price = 0.51
            )

            # 2) replace/ratchet each symbol’s order N times
            ratchet_ladder(
                app=app,
                ladder_state=ladder_state,
                step=step,              # how far to move price on each replace
                repeats=ratchet_repeats,
                price_quantum=PRICE_QUANTUM,
            )

            print("[MODE] ladder_multi done")


        # --- main logical main loop end ---

        # --- END TIMING ---
        end_ns = time.perf_counter_ns()
        end_dt = datetime.fromtimestamp(end_ns / 1_000_000_000)
        ms = end_dt.microsecond // 1000
        print(f"End time   : {end_dt.strftime('%H:%M:%S')}.{ms:03d}")

        elapsed_ns = end_ns - start_ns
        elapsed_ms = elapsed_ns / 1_000_000
        print(f"Elapsed (ms): {elapsed_ms:,.3f}")
        # simple math off the time stamp diference no lengthy calculations by the code to determine
        orders_per_sec = maxloop / (elapsed_ms / 1000.0)
        ops = (maxloop * 1000.0) / elapsed_ms
        print(f"Simple math for Orders/sec : {ops:,.2f}")

        
        print("Hit CTRL+C to log out and exit the app")
        while True:
            time.sleep(0.2)

    # keep session alive to receive ExecReports
    except KeyboardInterrupt:
        # graceful shutdown on Ctrl-C
        app.logout_and_stop(init, wait_secs=5)     # <<< qualify as method
    except Exception as e:
        print(f"[ERROR] Unexpected: {e}", file=sys.stderr)
        # ensure the engine stops even on errors
        app.logout_and_stop(init, wait_secs=2)     # <<< qualify as method
    finally:
        # safety net: if not already stopped, stop now
        if not stopped:
            try:
                init.stop()
            except Exception:
                pass
            stopped = True
    
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 MasterSendOrders.RPVersion.py <initiator.cfg> <simplerepeat|layer|replace>")
        sys.exit(1)

    print("Usage: python3 MasterSendOrders.RPVersion.py <initiator.cfg> "
              "<simplerepeat|layer|replace|ratchet|cancel|ladder_multi>")

    cfg = sys.argv[1]
    trademode = sys.argv[2].lower().strip()   

    if trademode not in {"simplerepeat", "layer", "replace", "ratchet", "cancel", "ladder_multi"}:
        print(f"Unknown mode: {trademode}")
        sys.exit(2)


    print(f"[CONFIG] mode={trademode} symbol={SYMBOL} "
      f"(multi={len(SYMBOLS)} strikes) "
      f"acct={ACCOUNT} subid={SENDER_SUB_ID} price={PRICE} qty={QTY} secsub={SecSubType}")

    input("Pause check above then hit enter")

    # make visible to main it sees the global values
    globals().update(dict(
            SENDER_SUB_ID=SENDER_SUB_ID,
            ACCOUNT=ACCOUNT,
            SYMBOL=SYMBOL,
            SecSubType=SecSubType,
            SIDE_BUY=SIDE_BUY,
            PRICE=PRICE,
            QTY=QTY,
            scope=scope,
            step=step,
            maxloop=maxloop,            
            ratchet_repeats=ratchet_repeats,
            ratchet_pause_s=ratchet_pause_s,
        ))

main(cfg, trademode)