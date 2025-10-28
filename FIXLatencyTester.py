#!/usr/bin/env python3
import time, uuid, threading, re, pytz, os, statistics, sys
from pathlib import Path
from datetime import datetime
from typing import Optional

# ===== Paths =====
rpdir = Path("/home/ec2-user/pythonQF")          # or Path.home(), Path.cwd(), etc.
data_dir = rpdir / "data"
log_file = rpdir / "logs" / "app.log"
rplog_file = rpdir / "logs" / "rpapp.log"
data_dir.mkdir(parents=True, exist_ok=True)
log_file.parent.mkdir(parents=True, exist_ok=True)

# ===== QuickFIX =====
import quickfix as fix
import quickfix50sp2 as fix50sp2


# ===== Config knobs =====
#tif            = "GOOD_TILL_CANCEL"
tif = "TimeInForce_DAY"
SENDER_SUB_ID  = "4C001"
# ACCOUNT      = "yesRonaldo"
# ACCOUNT      = "noRonaldo"
# ACCOUNT      = "yesTippy"
ACCOUNT        = "noTippy"
#SYMBOL        = "CBBTC_123125_65000"
#SYMBOL        = "CBBTC_123125_142500"
#SYMBOL        = "MNYCG_110425_Mamdani" 
SYMBOL         = "CBBTC_123125_132500"
#SecSubType     = "YES"
SecSubType     = "NO"
SIDE_BUY       = True  #always true 
QTY            = 1
maxloop        = 400
PRICE          = 0.50
incr           = 0.00
SUMMARY_EVERY  = 50      # print a stats line every N ExecReports captured

# ===== Latency tracker =====
class LatencyTracker:
    """Correlates ClOrdID -> send time; captures first ER latency per order."""
    def __init__(self, csv_path: Path):
        self._send_ns = {}           # clordid -> ns at send
        self._done    = set()        # clordids already recorded
        self._lock    = threading.Lock()
        self._lat_ms  = []           # list of float (milliseconds)
        self._count_reported = 0
        self.csv_path = csv_path
        if not self.csv_path.exists():
            # header
            self.csv_path.write_text("utc_ts,clordid,orderid,exectype,ordstatus,latency_ms,price,qty,symbol\n", encoding="utf-8")

    @staticmethod
    def _now_ns():
        # monotonic/steady clock for deltas
        return time.perf_counter_ns()

    def note_send(self, clordid: str):
        with self._lock:
            self._send_ns[clordid] = self._now_ns()
    
    

    def note_exec_report(self, clordid: str, orderid: str, exectype: str, ordstatus: str,
                     price: Optional[float], qty: Optional[float], symbol: Optional[str]):
        """Call on *any* ER. We record latency on first ER per clordid."""
        with self._lock:
            if clordid in self._done:
                return

            # We prefer to capture on PendingNew ('A') or New ('0'). If other types arrive first, we still record.
            if clordid not in self._send_ns:
                # We missed the send (e.g., restarted mid-stream). Ignore gracefully.
                return

            sent_ns = self._send_ns[clordid]
            delta_ms = (self._now_ns() - sent_ns) / 1_000_000.0
            self._lat_ms.append(delta_ms)
            self._done.add(clordid)

            # Append CSV row
            utc_iso = datetime.now(pytz.UTC).isoformat()
            row = f"{utc_iso},{clordid},{orderid},{exectype},{ordstatus},{delta_ms:.3f},{price if price is not None else ''},{qty if qty is not None else ''},{symbol or ''}\n"
            with self.csv_path.open("a", encoding="utf-8") as f:
                f.write(row)

            # periodic summary
            if len(self._done) // SUMMARY_EVERY > self._count_reported // SUMMARY_EVERY:
                self._count_reported = len(self._done)
                print(self.summary_line(prefix="[STATS]"))

    def summary(self):
        with self._lock:
            arr = list(self._lat_ms)
        if not arr:
            return {"n": 0}
        arr_sorted = sorted(arr)
        n = len(arr_sorted)
        mean = statistics.fmean(arr_sorted)
        p50 = arr_sorted[int(0.50*(n-1))]
        p90 = arr_sorted[int(0.90*(n-1))]
        p99 = arr_sorted[int(0.99*(n-1))]
        mx  = arr_sorted[-1]
        return {"n": n, "mean": mean, "p50": p50, "p90": p90, "p99": p99, "max": mx}

    def summary_line(self, prefix=""):
        s = self.summary()
        if s.get("n", 0) == 0:
            return f"{prefix} n=0 (no samples yet)"
        return (f"{prefix} n={s['n']}  mean={s['mean']:.2f}ms  p50={s['p50']:.2f}ms  "
                f"p90={s['p90']:.2f}ms  p99={s['p99']:.2f}ms  max={s['max']:.2f}ms")

# ===== App =====
class App(fix.Application):
    def __init__(self):
        super().__init__()
        self.session_id = None
        self.lat = LatencyTracker(data_dir / "latency.csv")

    # lifecycle
    def onCreate(self, sid): pass
    
    def onLogon(self, sid):
        utc_time = datetime.now(pytz.UTC)
        est_time = datetime.now(pytz.timezone('US/Eastern'))
        print(f"[LOGON] Session ID: {sid} at {est_time.strftime('%H:%M:%S %Z')} or {utc_time.strftime('%H:%M:%S %Z')}")
        self.session_id = sid
        
    def onLogout(self, sid):
        utc_time = datetime.now(pytz.UTC)
        est_time = datetime.now(pytz.timezone('US/Eastern'))
        print(f"[COMPLETION] Workflow finished at {utc_time.strftime('%H:%M:%S %Z')} / {est_time.strftime('%H:%M:%S %Z')}")
        print(self.lat.summary_line(prefix="[FINAL]"))

    # admin plumbing
    def toAdmin(self, msg, sid):
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_Logon:
            try: msg.getHeader().removeField(50)
            except fix.FieldNotFound: pass
            msg.setField(fix.EncryptMethod(0))        # 98=0
            msg.setField(fix.HeartBtInt(30))          # 108=30
            msg.setField(fix.DefaultApplVerID("9"))   # 1137=9 (FIX50SP2)
            msg.setField(fix.ResetSeqNumFlag(True))   # 141=Y
    
    def fromAdmin(self, msg, sid): pass

    # app plumbing
    def toApp(self, msg, sid):
        # Add SenderSubID(50) to *all* application messages
        msg.getHeader().setField(fix.SenderSubID(SENDER_SUB_ID))

        # If this is our outgoing NOS (35=D), remember send time keyed by ClOrdID(11)
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_NewOrderSingle:
            try:
                cl = fix.ClOrdID(); msg.getField(cl)
                self.lat.note_send(cl.getValue())
            except fix.FieldNotFound:
                pass

    def fromApp(self, msg, sid):
        # print & raw-log everything we receive
        wire = msg.toString()
        print("[APP]", wire)
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(wire + "\n")

        # For ExecReports (35=8), compute latency for the first ER per ClOrdID.
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_ExecutionReport:
            # Pull correlation fields
            cl_val = None
            try:
                cl = fix.ClOrdID(); msg.getField(cl); cl_val = cl.getValue()
            except fix.FieldNotFound:
                pass

            order_id = None
            try:
                oid = fix.OrderID(); msg.getField(oid); order_id = oid.getValue()
            except fix.FieldNotFound:
                pass

            exectype = None
            try:
                et = fix.ExecType(); msg.getField(et); exectype = et.getValue()
            except fix.FieldNotFound:
                pass

            ordstatus = None
            try:
                osf = fix.OrdStatus(); msg.getField(osf); ordstatus = osf.getValue()
            except fix.FieldNotFound:
                pass

            # Optional – for CSV detail
            price = None
            try:
                pr = fix.Price(); msg.getField(pr); price = float(pr.getValue())
            except fix.FieldNotFound:
                pass

            qty = None
            try:
                oq = fix.OrderQty(); msg.getField(oq); qty = float(oq.getValue())
            except fix.FieldNotFound:
                pass

            symbol = None
            try:
                sy = fix.Symbol(); msg.getField(sy); symbol = sy.getValue()
            except fix.FieldNotFound:
                pass

            if cl_val:
                self.lat.note_exec_report(cl_val, order_id, exectype or "", ordstatus or "", price, qty, symbol)

    # ===== Actions =====
    def send_gtc_limit(self, symbol, buy, qty, price, sec_subtype, account=None):
        nos = fix50sp2.NewOrderSingle()
        nos.setField(fix.ClOrdID("CL-" + str(uuid.uuid4())))               # 11
        if account: nos.setField(fix.Account(account))                      # 1
        nos.setField(fix.Symbol(symbol))                                    # 55
        nos.setField(fix.Side(fix.Side_BUY if buy else fix.Side_SELL))      # 54
        nos.setField(fix.TransactTime())                                    # 60 (now)
        nos.setField(fix.OrdType(fix.OrdType_LIMIT))                        # 40=2
        nos.setField(fix.OrderQty(float(qty)))                              # 38
        nos.setField(fix.Price(float(price)))                               # 44
        nos.setField(fix.CustOrderCapacity(1))                              # 582
        nos.setField(fix.AccountType(1))                                    # 581
        nos.setField(fix.SecuritySubType(sec_subtype))                      # 762
        nos.setField(fix.TimeInForce(fix.TimeInForce_DAY))                  # 59=1 (TID)
        #nos.setField(fix.TimeInForce(fix.TimeInForce_GOOD_TILL_CANCEL))     # 59=1 (GTC)
        ok = fix.Session.sendToTarget(nos, self.session_id)
        print(f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} {sec_subtype} -> {ok}")
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} {sec_subtype} -> {ok}\n")

# ===== Main =====
def main(cfg):
    settings = fix.SessionSettings(cfg)
    app = App()
    store = fix.FileStoreFactory(settings)
    logs  = fix.FileLogFactory(settings)
    init = fix.SocketInitiator(app, store, settings, logs)
    
    init.start()

    try:
        while app.session_id is None:
            time.sleep(0.05)

        i = 1
        new_price = PRICE
        while i <= maxloop:
            new_price += incr
            app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, new_price, "YES", ACCOUNT)
            app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, new_price, "NO", ACCOUNT)
            time.sleep(0.01)
            if i % 50 == 0:
                # mid-run stats pulse
                print(app.lat.summary_line(prefix="[STATS]"))
            i += 1

        # keep session alive to receive ExecReports
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
         summary(self)
         summary_line(self, prefix="")

    finally:
        print(app.lat.summary_line(prefix="[FINAL]"))
        init.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv)!=2:
        print("Usage: python ep3_limit_gtc.py initiator.cfg"); raise SystemExit(1)
    main(sys.argv[1])