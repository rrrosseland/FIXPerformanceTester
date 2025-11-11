#!/usr/bin/env python3
import time, uuid, threading, re, pytz, sys, statistics, datetime, threading, queue, collections

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

# Build child paths
data_dir = rpdir / "data"
log_file = rpdir / "logs" / "app.log"
rplog_file = rpdir / "logs" / "rpapp.log"

# -------------------- USER DEFAULTS (edit here) --------------------
SENDER_SUB_ID = "4C001"      # Session / identity / never changes 
SIDE_BUY = True              # our products are always BUY, keep True

# Trading controls
ACCOUNT = "yesRonaldo"
# ACCOUNT = "noRonaldo"
# ACCOUNT = "noTippy"
# ACCOUNT = "yesTippy"
# ACCOUNT = "RPTEST"
SYMBOL = "CBBTC_123125_132500"
# SYMBOL = "CBBTC_123125_65000"
# SYMBOL = "CBBTC_123125_142500"
# SYMBOL = "MNYCG_110425_Mamdani"
SecSubType   = "YES"
# SecSubType = "NO"

# Mode-independent numeric defaults (CLI can override per-mode later)
PRICE        = 0.52
QTY          = 1

# Mode: layer
scope        = 0.10             # total ladder range
step         = 0.01             # ladder increment

# Mode: simplerepeat, layer, replace and ratchet
maxloop      = 10               # how many times my outer loop runs per mode - count - attempts

# Mode: layer, replace and ratchet
bump         = 0.01             # price change per replace
ratchet_repeats = 10            # number of replaces
ratchet_pause_s = 0.20          # seconds between replaces
# -------------------------------------------------------------------

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
        
    def onLogout(self, sid):
        self.logged_on = False
        print(f"[onLogout] Logged out: {sid}")
        # keep the last session_id so we can re-use on reconnect if needed to hold the logout...
    
    # this function makes the CTRL+C close cleanly because we never call onLogout due to it's requirement with QuickFIX
    def logout_and_stop(self, init, wait_secs=5):        
        try:
            if app.session_id:
                sess = fix.Session.lookupSession(app.session_id)
                if sess:
                    sess.logout("Client requested shutdown")
            t0 = time.time()
            while app.logged_on and (time.time() - t0) < wait_secs:
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
        nos.setField(fix.ClOrdID(cl))                         # 11
        if account:    nos.setField(fix.Account(account))     # 1
        nos.setField(fix.Symbol(symbol))                      # 55
        nos.setField(fix.Side(fix.Side_BUY if buy else fix.Side_SELL))  # 54
        nos.setField(fix.TransactTime())                      # 60
        nos.setField(fix.OrdType(fix.OrdType_LIMIT))          # 40=2
        nos.setField(fix.OrderQty(float(qty)))                # 38
        nos.setField(fix.Price(float(price)))                 # 44
        nos.setField(fix.CustOrderCapacity(1))                # 582
        nos.setField(fix.AccountType(1))                      # 581
        if sec_subtype: nos.setField(fix.SecuritySubType(sec_subtype))  # 762
        if tif is not None: nos.setField(fix.TimeInForce(tif))          # 59
        else:               nos.setField(fix.TimeInForce(fix.TimeInForce_DAY))

        ok = fix.Session.sendToTarget(nos, self.session_id)

        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(f"[SEND] LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} {sec_subtype} -> {ok}\n")

        # Track
        self.tracker.register_new(TrackedOrder(
            symbol=symbol,
            side=(fix.Side_BUY if buy else fix.Side_SELL),
            qty=float(qty),
            tif=tif,
            account=account,
            sec_subtype=sec_subtype,
            first_clordid=cl,
            last_clordid=cl,
            price=float(price),
            live=False
        ))
        return cl

        # Track it
        self.tracker.register_new(TrackedOrder(
            symbol=symbol,
            side=(fix.Side_BUY if buy else fix.Side_SELL),
            qty=float(qty),
            tif=tif,
            account=account,
            sec_subtype=sec_subtype,
            first_clordid=cl,
            last_clordid=cl,
            price=float(price),
            live=False
        ))
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

    def send_replace(self, last_clordid, new_price=None, new_qty=None):
        o = self.tracker.get_by_last_clordid(last_clordid)
        if not o or not o.live:
            print(f"[WARN] Cannot replace; order not live or unknown: {last_clordid}")
            return None

        new_cl = f"CR-{uuid.uuid4()}"
        rep = fix50sp2.OrderCancelReplaceRequest()
        rep.setField(fix.OrigClOrdID(o.last_clordid))  # 41
        rep.setField(fix.ClOrdID(new_cl))              # 11
        rep.setField(fix.Symbol(o.symbol))             # 55
        rep.setField(fix.Side(o.side))                 # 54
        rep.setField(fix.TransactTime())               # 60

        # Include OrderID if we have it—reduces DK risk
        if o.order_id:
            rep.setField(fix.OrderID(o.order_id))      # 37

        # Price/Qty changes (many venues want qty restated even if unchanged)
        rep.setField(fix.OrderQty(float(new_qty if new_qty is not None else o.qty)))  # 38
        if new_price is not None:
            rep.setField(fix.Price(float(new_price)))  # 44
        elif o.price is not None:
            rep.setField(fix.Price(float(o.price)))

        # Restate these if your venue requires (often yes)
        if o.tif is not None:       rep.setField(fix.TimeInForce(o.tif))        # 59
        if o.account:               rep.setField(fix.Account(o.account))        # 1
        if o.sec_subtype:           rep.setField(fix.SecuritySubType(o.sec_subtype))  # 762

        fix.Session.sendToTarget(rep, self.session_id)
        self.tracker.mark_pending_replace(o.last_clordid)
        return new_cl

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

            # 3) selective replace (+bump) only if order is live and not done
            replaced, skipped = 0, 0
            for cl in created:
                o = app.tracker.get_by_last_clordid(cl)
                if not o or not o.live:
                    skipped += 1
                    continue
                new_px = (o.price if o.price is not None else PRICE) + bump
                new_cl = app.send_replace(cl, new_price=new_px)
                if new_cl:
                    replaced += 1

            print(f"[replace] sent={len(created)} replaces={replaced} skipped={skipped}")
        
        elif trademode == "ratchet":
            print("[MODE] ratchet: 1 new order, then replace it repeatedly")

            # 1) Send one NEW order
            base_px = PRICE
            cl = app.send_limit(SYMBOL, SIDE_BUY, QTY, base_px, SecSubType, ACCOUNT)

            # 2) Wait briefly for NEW ack (ExecutionReport 35=8 / 150=0)
            app.wait_for_exec(cl, want_exec_types=("0",), timeout=1.0)

            current_cl = cl
            replaces_ok = 0
            for k in range(ratchet_repeats):
                # compute next price; you can choose linear or oscillating
                next_px = base_px + (k + 1) * bump

                # (optional) verify the order is live before each replace
                o = app.tracker.get_by_last_clordid(current_cl)
                if not o or not o.live:
                    print(f"[ratchet] stop: order not live (cl={current_cl}) at step {k}")
                    break

                # 3) Send REPLACE (35=G) and wait for REPLACE ack (150=5)
                new_cl = app.send_replace(current_cl, new_price=next_px)
                if not new_cl:
                    print(f"[ratchet] replace send failed at step {k}")
                    break

                ev = app.wait_for_exec(new_cl, want_exec_types=("5",), timeout=1.0)
                if ev is None:
                    print(f"[ratchet] no replace ACK within timeout at step {k} (cl={new_cl})")
                    # you can continue or break; continuing keeps pressure on the gateway
                    # break
                else:
                    replaces_ok += 1

                current_cl = new_cl
                time.sleep(ratchet_pause_s)

            print(f"[ratchet] done: replaces_ok={replaces_ok}/{ratchet_repeats}")

        # --- main logical main loop end ---

        # --- END TIMING ---
        end_ns = time.perf_counter_ns()
        end_dt = datetime.fromtimestamp(end_ns / 1_000_000_000)
        ms = end_dt.microsecond // 1000
        print(f"End time   : {end_dt.strftime('%H:%M:%S')}.{ms:03d}")
        elapsed_ns = end_ns - start_ns
        elapsed_ms = elapsed_ns / 1_000_000
        print(f"Elapsed (ms): {elapsed_ms:,.3f}")
        
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

    cfg = sys.argv[1]
    trademode = sys.argv[2].lower().strip()   

    if trademode not in {"simplerepeat", "layer", "replace", "ratchet"}:
        print(f"Unknown mode: {trademode}")
        sys.exit(2)

    print(f"[CONFIG] mode={trademode} symbol={SYMBOL} acct={ACCOUNT} subid={SENDER_SUB_ID} "
        f"price={PRICE} qty={QTY} secsub={SecSubType}")
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
            bump=bump,
            ratchet_repeats=ratchet_repeats,
            ratchet_pause_s=ratchet_pause_s,
        ))

main(cfg, trademode)