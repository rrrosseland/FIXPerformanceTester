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

# cfg = sys.argv[1]
# trademode = sys.argv[2].lower()

if trademode == "simplerepeat":
    PRICE  = 0.52
    QTY = 1
    maxloop = 5
elif trademode == "layer":
    PRICE   = 0.52       # starting price
    QTY     = 1          # quantity per order
    maxloop = 5      # number of up/down passes on the ladder
    scope   = 0.10       # total range, e.g. 0.52–0.62
    step    = 0.01       # increment size per layer
elif trademode == "replace":
    PRICE   = 0.52
    QTY     = 1
    maxloop = 5
    bump    = 0.01       # how much to change the price on each replace

print("The Price is: ", PRICE, "and trademode is: ", trademode)
user_input = input("Pause check above then hit enter")   #pause before executing just in case

# Build child paths
data_dir = rpdir / "data"
log_file = rpdir / "logs" / "app.log"
rplog_file = rpdir / "logs" / "rpapp.log"

# arguments:
SENDER_SUB_ID  = "4C001"
ACCOUNT = "yesRonaldo"
# ACCOUNT = "noRonaldo"
# ACCOUNT = "noTippy"
# ACCOUNT = "yesTippy"
# ACCOUNT = "RPTEST"
# SYMBOL = "CBBTC_123125_65000"
# SYMBOL = "CBBTC_123125_142500"
# SYMBOL = "MNYCG_110425_Mamdani" 
SYMBOL = "CBBTC_123125_132500"
SecSubType = "YES"
# SecSubType = "NO"
SIDE_BUY = True  # this is always true with our products

@dataclass
class TrackedOrder:
    symbol: str
    side: int            # fix.Side_BUY / fix.Side_SELL
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
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(msg.toString() + "\n")
         # 2) minimally track order state for replace support
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() != fix.MsgType_ExecutionReport:   # only care about 35=8
            return

        # read the few fields we need (guard each in case absent)
        exec_type = fix.ExecType();     msg.getField(exec_type)          # 150
        clordid   = fix.ClOrdID();      msg.getField(clordid)            # 11
        cl        = clordid.getValue()
        cl11 = fix.ClOrdID();    msg.getField(cl11); cl = cl11.getValue()
        et   = fix.ExecType();   msg.getField(et);   exec_type = et.getValue()
        st   = None
        order_id = None

        try:
            oid = fix.OrderID(); msg.getField(oid)                       # 37
            order_id = oid.getValue()
        except fix.FieldNotFound:
            pass

        if exec_type.getValue() == fix.ExecType_NEW:                     # 150=0
            self.tracker.on_ack_new(cl, order_id)

        elif exec_type.getValue() == fix.ExecType_REPLACE:               # 150=5
            try:
                orig = fix.OrigClOrdID(); msg.getField(orig)             # 41
                new_px = None
                try:
                    px = fix.Price(); msg.getField(px)                   # 44
                    new_px = float(px.getValue())
                except fix.FieldNotFound:
                    pass
                self.tracker.on_ack_replace(orig.getValue(), cl, new_px)
            except fix.FieldNotFound:
                # Replace ack without 41 is unusual; just ignore gracefully
                pass
        # signal anyone waiting on this ClOrdID
        self._ev[cl].put({
            "exec_type": exec_type,   # e.g., fix.ExecType_REPLACE (='5')
            "ord_status": st,         # '0','1','3','4',...
            "clordid": cl
        })
    
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

    def send_limit(self, symbol, buy, qty, price, SecSubType, account=None):
        nos = fix50sp2.NewOrderSingle()
        nos.setField(fix.ClOrdID("CL-" + str(uuid.uuid4())))               # 11
        if account: nos.setField(fix.Account(account))                     # 1
        nos.setField(fix.Symbol(symbol))                                   # 55
        nos.setField(fix.Side(fix.Side_BUY))                               # 54 always BUY
        nos.setField(fix.TransactTime())                                   # 60 (now)
        nos.setField(fix.OrdType(fix.OrdType_LIMIT))                       # 40=2
        nos.setField(fix.OrderQty(float(qty)))                             # 38
        nos.setField(fix.Price(float(price)))                              # 44
        nos.setField(fix.CustOrderCapacity(1))                             # 582 = 5 (RETAIL
        nos.setField(fix.AccountType(1))                                   # 581 
        nos.setField(fix.SecuritySubType(SecSubType))                      # 762 Required for YES NO
        # tif = TimeInForce_GOOD_TILL_CANCEL
        # tif = TimeInForce_DAY ###to move tif outside this def, need to add as a param of def ##
        # nos.setField(fix.TimeInForce(fix.TimeInForce_GOOD_TILL_CANCEL))  # 59=1 (GTC)
        # nos.setField(fix.TimeInForce(fix.tif)    # DAY 59=0 (DAY)
        nos.setField(fix.TimeInForce(fix.TimeInForce_DAY))                 # DAY 59=0 (DAY)
        ok = fix.Session.sendToTarget(nos, self.session_id)
        # nos.setField(fix.TimeInForce(0))                                 # 59=1 (GTC) and DAY = 0 THIS DOES NOT WORK!!!
        # print(f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} {SecSubType} -> {ok}")
        msgstrrp = (f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} {SecSubType} -> {ok}")
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(msgstrrp + "\n")
         # your header fields, SenderSubID(50), etc. are set in toApp() or elsewhere

        fix.Session.sendToTarget(nos, self.session_id)

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

    # Mode defaults
    PRICE = 0.52
    QTY = 1
    maxloop = 10
    bump = 0.01
    scope = 0.10
    step = 0.01

    if trademode not in {"simplerepeat", "layer", "replace"}:
        print(f"Unknown mode: {trademode}")
        sys.exit(2)

    print("The Price is:", PRICE, "and trademode is:", trademode)
    input("Pause check above then hit enter")

# stash in globals the few items main uses
globals().update(dict(PRICE=PRICE, QTY=QTY, maxloop=maxloop, bump=bump, scope=scope, step=step))
main(cfg, trademode)