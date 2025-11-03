#!/usr/bin/env python3
import time, uuid, threading, re, pytz, sys, statistics, datetime
import quickfix as fix
import quickfix50sp2 as fix50sp2
from pathlib import Path
from datetime import datetime
rpdir = Path("/home/ec2-user/pythonQF")

cfg = sys.argv[1]
trademode = sys.argv[2]

if trademode == "simpleRepeat":
    PRICE  = 0.49
    QTY = 1
    maxloop = 100
    incr = 0.00

print("The trademode is: ", trademode)
user_input = input("Pause check above then hit enter")

# Build child paths
data_dir = rpdir / "data"
log_file = rpdir / "logs" / "app.log"
rplog_file = rpdir / "logs" / "rpapp.log"

# arguments:
SENDER_SUB_ID  = "4C001"
ACCOUNT = "yesRonaldo"
#ACCOUNT = "noRonaldo"
# ACCOUNT = "noTippy"
# ACCOUNT = "yesTippy"
# ACCOUNT = "RPTEST"
SYMBOL = "CBBTC_123125_132500"
# SYMBOL = "CBBTC_123125_142500"
# SYMBOL = "CBBTC_123125_65000"
# SYMBOL = "MNYCG_110425_Mamdani" 
SecSubType = "YES"
#SecSubType = "NO"
SIDE_BUY = True  # set False for sell

class App(fix.Application):
    def __init__(self):
        super().__init__()
        self.session_id = None
        self.logged_on = False
        self.run_mode = ""
        self.run_symbol = ""
        self.run_secsub = ""
        # --- market data tracking ---
        self._md_pending = {}   # MDReqID -> threading.Event()
        self._md_last = {}      # MDReqID -> dict(snapshot data)    
        
    def onCreate(self, sid):
        print(f"[onCreate] Session created: {sid}")
    
    def onLogon(self, sid):
        tgt = sid.getTargetCompID().getValue()
        if tgt == "ForecastExMD":
            self.md_session = sid
            print(f"[onLogon] Logged on MD: {sid}")
            if getattr(self, "run_mode", "") == "mdpoll":
                self.send_md_subscribe(self.run_symbol, SecSubType=self.run_secsub,
                                    depth=1, want_trade=True, incremental=True)
        else:
            self.trade_session = sid
            print(f"[onLogon] Logged on TRADING: {sid}")
        
    def onLogout(self, sid):
        self.logged_on = False
        print(f"[onLogout] Logged out: {sid}")
        # keep the last session_id so we can re-use on reconnect if needed to hold the logout...

    def toAdmin(self, msg, sid):
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        print(f"[toAdmin] sending mt {mt}")
        if mt.getValue() == fix.MsgType_Logon:
            # venue-required fields (keep your existing lines here)
            try: msg.getHeader().removeField(50)
            except fix.FieldNotFound: pass
            msg.setField(fix.EncryptMethod(0))
            msg.setField(fix.HeartBtInt(30))
            msg.setField(fix.DefaultApplVerID("9"))
            msg.setField(fix.ResetSeqNumFlag(True))
        # print every outbound admin
        print(f"[OUT ADMIN] {msg.toString()}")
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write("[OUT ADMIN] " + msg.toString() + "\n")
    
    def fromAdmin(self, msg, sid):
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        mtype = mt.getValue()
        s = msg.toString()
        print(f"[IN ADMIN]  {s}")
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write("[IN ADMIN]  " + s + "\n")
        # Show reason text for rejects/logouts
        if mtype in ('3', '5', 'j', fix.MsgType_Reject, fix.MsgType_Logout, fix.MsgType_BusinessMessageReject):
            try:
                txt = fix.Text(); msg.getField(txt)
                print(f"[ADMIN REASON] 58={txt.getValue()}")
            except fix.FieldNotFound:
                pass

    # app plumbing
    def toApp(self, msg, sid):
        # Add SenderSubID(50) to *all* application messages
        msg.getHeader().setField(fix.SenderSubID(SENDER_SUB_ID))
        # DEBUG: print any outbound app message (35 != A,5 etc.)
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        mtype = mt.getValue()
        s = msg.toString()
        print(f"[OUT APP]   {s}")     # <-- shows 35=V, 263=1, 55, 762 etc.
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write("[OUT APP]   " + s + "\n")

    def fromApp(self, msg, sid):
        # Raw line to file + screen
        raw = msg.toString()
        print(f"[IN APP] {raw}")
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write("[IN APP] " + raw + "\n")

        # MsgType
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        mtype = mt.getValue()  # 'W' snapshot, 'X' incremental, 'Y' MDReqRej (FIX42/44/50sp2)

        # Try to grab 262 on any MD message
        mdreqid = ""
        try:
            v = fix.MDReqID(); msg.getField(v)
            mdreqid = v.getValue()
        except fix.FieldNotFound:
            pass

        # ---- MarketDataSnapshotFullRefresh (35=W) ----
        if mtype == fix.MsgType_MarketDataSnapshotFullRefresh:  # 'W'
            # 55
            symbol = ""
            try:
                sym = fix.Symbol(); msg.getField(sym)
                symbol = sym.getValue()
            except fix.FieldNotFound:
                pass

            # 268 NoMDEntries
            book = []
            try:
                n = fix.NoMDEntries(); msg.getField(n)
                count = int(n.getValue())
                for i in range(1, count + 1):
                    # IMPORTANT: new group object per row
                    grp = fix50sp2.MarketDataSnapshotFullRefresh.NoMDEntries()
                    msg.getGroup(i, grp)
                    entry = {}
                    # 269 type
                    try:
                        t = fix.MDEntryType(); grp.getField(t)
                        entry["type"] = t.getValue()  # '0'=Bid, '1'=Offer, '2'=Trade
                    except fix.FieldNotFound:
                        pass
                    # 270 px
                    try:
                        px = fix.MDEntryPx(); grp.getField(px)
                        entry["px"] = float(px.getValue())
                    except fix.FieldNotFound:
                        pass
                    # 271 sz
                    try:
                        sz = fix.MDEntrySize(); grp.getField(sz)
                        entry["sz"] = float(sz.getValue())
                    except fix.FieldNotFound:
                        pass
                    book.append(entry)
            except fix.FieldNotFound:
                # No 268 — some venues may send an empty snapshot header
                pass

            snapshot = {"symbol": symbol, "entries": book, "raw": raw}
            if mdreqid:
                self._md_last[mdreqid] = snapshot
                # wake waiter for this specific request
                ev = self._md_pending.get(mdreqid)
                if ev:
                    ev.set()

            line = f"[MD SNAP] 35=W 262={mdreqid or 'NA'} 55={symbol or 'NA'} entries={len(book)}"
            print(line)
            with rplog_file.open("a", encoding="utf-8") as f:
                f.write(line + "\n")

            return  # handled

        # ---- MarketDataIncrementalRefresh (35=X) ----
        if mtype == fix.MsgType_MarketDataIncrementalRefresh:  # 'X'
            updates = []
            try:
                n = fix.NoMDEntries(); msg.getField(n)
                count = int(n.getValue())
                for i in range(1, count + 1):
                    grp = fix50sp2.MarketDataIncrementalRefresh.NoMDEntries()
                    msg.getGroup(i, grp)
                    upd = {}
                    try:
                        t = fix.MDUpdateAction(); grp.getField(t)
                        upd["act"] = t.getValue()  # 0=new,1=change,2=delete
                    except fix.FieldNotFound:
                        pass
                    try:
                        et = fix.MDEntryType(); grp.getField(et)
                        upd["type"] = et.getValue()  # '0','1','2',...
                    except fix.FieldNotFound:
                        pass
                    try:
                        px = fix.MDEntryPx(); grp.getField(px)
                        upd["px"] = float(px.getValue())
                    except fix.FieldNotFound:
                        pass
                    try:
                        sz = fix.MDEntrySize(); grp.getField(sz)
                        upd["sz"] = float(sz.getValue())
                    except fix.FieldNotFound:
                        pass
                    # optional symbol at entry level (some feeds include it)
                    try:
                        sym = fix.Symbol(); grp.getField(sym)
                        upd["sym"] = sym.getValue()
                    except fix.FieldNotFound:
                        pass
                    updates.append(upd)
            except fix.FieldNotFound:
                pass

            line = f"[MD INC] 35=X 262={mdreqid or 'NA'} n={len(updates)}"
            print(line)
            with rplog_file.open("a", encoding="utf-8") as f:
                f.write(line + " " + str(updates) + "\n")

            return  # handled

        # ---- MarketDataRequestReject (35=Y) ----
        if mtype == 'Y':  # quickfix lacks a constant alias for Y in some builds
            reason_txt = ""
            try:
                txt = fix.Text(); msg.getField(txt); reason_txt = txt.getValue()
            except fix.FieldNotFound:
                pass
            rej_code = ""
            try:
                c = fix.MDReqRejReason(); msg.getField(c); rej_code = str(c.getValue())
            except fix.FieldNotFound:
                pass

            line = f"[MD REJ] 35=Y 262={mdreqid or 'NA'} reason={rej_code or 'NA'} text={reason_txt or ''}"
            print(line)
            with rplog_file.open("a", encoding="utf-8") as f:
                f.write(line + "\n")

            # if we were waiting on this mdreqid, fail-fast the waiter
            if mdreqid:
                ev = self._md_pending.get(mdreqid)
                if ev:
                    ev.set()  # wake; caller can inspect _md_last or see the REJ line
            return

        # ---- Other app messages (ExecReports, etc.) fall through ----
        # (already logged above)

    #--------------Main sendorder build
    def send_limit(self, Symbol, Buy, Qty, Price, SecSubType, Account=None):
        nos = fix50sp2.NewOrderSingle()

        clid = "CL-" + str(uuid.uuid4())
        nos.setField(fix.ClOrdID(clid))                                # 11

        if Account:
            nos.setField(fix.Account(Account))                         # 1

        nos.setField(fix.Symbol(Symbol))                               # 55
        nos.setField(fix.Side(fix.Side_BUY if Buy else fix.Side_SELL)) # 54
        nos.setField(fix.TransactTime())                               # 60 (now)
        nos.setField(fix.OrdType(fix.OrdType_LIMIT))                   # 40=2
        nos.setField(fix.OrderQty(float(Qty)))                         # 38
        nos.setField(fix.Price(float(Price)))                          # 44

        # venue-specific tags (keep as you had them)
        nos.setField(fix.CustOrderCapacity(1))                        # 582
        nos.setField(fix.AccountType(1))                              # 581
        nos.setField(fix.SecuritySubType(SecSubType))                 # 762

        # Time in force: DAY (59=0). Your log should match this.
        nos.setField(fix.TimeInForce(fix.TimeInForce_DAY))            # 59=0

        # Prefer the trading session if you have two sessions configured
        sid = getattr(self, "trade_session", None) or self.session_id
        ok = fix.Session.sendToTarget(nos, sid)

        msg = f"[SEND] LIMIT {Symbol} {('BUY' if Buy else 'SELL')} {Qty} @ {Price} {SecSubType} (TIF=DAY) -> {ok} 11={clid}"
        print(msg)
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")

        return clid, ok

    #--------------Main Market Data section Start
    #............................................
    def send_md_subscribe(self, Symbol, SecSubType="YES", Depth=1, WantTrade=True, Incremental=True):
        if not getattr(self, "md_session", None):
            raise RuntimeError("MD session not logged on yet")

        MDReqID = f"MD-{uuid.uuid4()}"

        md = fix50sp2.MarketDataRequest()
        md.setField(fix.MDReqID(MDReqID))                     # 262
        md.setField(fix.SubscriptionRequestType('1'))         # 263=1
        md.setField(fix.MarketDepth(Depth))                   # 264
        md.setField(fix.MDUpdateType(1 if Incremental else 0))# 265
        md.setField(fix.AggregatedBook(True))                 # 266=Y

        types = [fix.MDEntryType_BID, fix.MDEntryType_OFFER]
        if WantTrade:
            types.append(fix.MDEntryType_TRADE)
        for et in types:
            g = fix50sp2.MarketDataRequest.NoMDEntryTypes()
            g.setField(fix.MDEntryType(et))
            md.addGroup(g)

        rel = fix50sp2.MarketDataRequest.NoRelatedSym()
        rel.setField(fix.Symbol(Symbol))                      # 55
        rel.setField(fix.SecuritySubType(SecSubType))         # 762
        md.addGroup(rel)

        ok = fix.Session.sendToTarget(md, self.md_session)
        print(f"[SEND MD SUB] 35=V 263=1 262={MDReqID} 55={Symbol} 762={SecSubType} -> {ok}")
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(f"[SEND MD SUB] 35=V 263=1 262={MDReqID} 55={Symbol} 762={SecSubType} -> {ok}\n")
        return MDReqID   

    def send_md_unsubscribe(self, mdreqid: str):
        md = fix50sp2.MarketDataRequest()
        md.setField(fix.MDReqID(mdreqid))                 # 262
        md.setField(fix.SubscriptionRequestType('2'))     # 263=2 unsubscribe
        # spec requires 267/146 to match original in some venues; include them if EP3 requires
        ok = fix.Session.sendToTarget(md, self.session_id)
        print(f"[SEND MD UNSUB] 262={mdreqid} -> {ok}")	

def main(cfg, trademode):
    settings = fix.SessionSettings(cfg)
    app = App()
    app.run_mode = trademode.lower()
    app.run_symbol = SYMBOL
    app.run_secsub = SecSubType
    r3wall_start_secs = datetime.now()
    times = []   # <-- before your while loop chat
    store = fix.FileStoreFactory(settings)
    # logs  = fix.FileLogFactory(settings)
    logs  = fix.ScreenLogFactory(True, True, True)  # show incoming, outgoing, events on stdout
    init  = fix.SocketInitiator(app, store, settings, logs)
    init.start()
    run_start = time.perf_counter()

    #### DAY ORDERS will get CXLD upon sesson logout. GTC will persist
    try:
        # wait for logon then fire orders
        while app.session_id is None:
            time.sleep(0.1)        
        i = 1
        NEWPRICE  = PRICE

        # ---------- NEW: market data polling mode ----------
        if trademode.lower() == "mdpoll":
            print(f"[MD MODE] SUBSCRIBE 35=V 263=1 for 55={SYMBOL} 762={SecSubType}")
            print(f"[MD MODE] subscribe 55={SYMBOL} 762={SecSubType}")
            mdreqid = app.send_md_subscribe(SYMBOL, SecSubType, depth=1, want_trade=True, incremental=True)
            app.send_md_subscribe(SYMBOL, SecSubType, depth=1, want_trade=True, incremental=True)
            print(f"[MD MODE] polling 35=V snapshots for {SYMBOL} every 30s")
            interval_sec = 30.0
            while True:
                t0 = time.perf_counter()
                time.sleep(interval_sec)
                print(f"[MD MODE] refresh subscribe 262(new)")
                mdreqid = app.send_md_subscribe(SYMBOL, SecSubType, depth=1, want_trade=True, incremental=True)
                mdreqid, ev = app.send_md_snapshot(SYMBOL, depth=1, want_trade=True)
                app.refresh_md_subscription(SYMBOL, SecSubType, depth=1, want_trade=True, incremental=True)
                # wait up to 5 seconds for a snapshot (W)
                got = ev.wait(timeout=5.0)
                if not got:
                    print(f"[MD WARN] timed out waiting for snapshot 262={mdreqid}")
                else:
                    snap = app._md_last.get(mdreqid, {})
                    # quick pretty print to stdout
                    entries = snap.get("entries", [])
                    bid = next((e for e in entries if e.get("type") == '0'), None)
                    ask = next((e for e in entries if e.get("type") == '1'), None)
                    trade = next((e for e in entries if e.get("type") == '2'), None)
                    print(f"[MD] 55={SYMBOL} "
                            f"bid={bid.get('px') if bid else None} "
                            f"ask={ask.get('px') if ask else None} "
                            f"last={trade.get('px') if trade else None} "
                            f"n={len(entries)} 262={mdreqid}")

                # sleep to align to ~30s cadence from send time
                elapsed = time.perf_counter() - t0
                wait = max(0.0, interval_sec - elapsed)
                time.sleep(wait)
            # (never returns)

        # ---------- existing order modes (unchanged) ----------
        i = 1
        NEWPRICE = PRICE
        print("before the loop here are the values:", SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
        
        print("before the loop here are the values:", SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
        # layer up the lower half of book, then the upper half or start at 99 and go down in increm
        while i <= maxloop:
            start = time.perf_counter()   # <-- define start at the top of each iteration
            NEWPRICE = NEWPRICE + incr
            if trademode == "latencyTest":
                app.send_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
            elif trademode == "layerLower45s":
                app.send_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
            elif trademode == "simpleRepeat":            
                app.send_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
            elif trademode in ("md", "mdpoll", "marketdata"):
                trademode = "mdpoll"
            else:
                app.send_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)

            elapsed = (time.perf_counter() - start) * 1000.0  # ms
            times.append(elapsed)

            # optional throttle / logging — safe to remove for raw throughput
            time.sleep(0.00001)
            i += 1
        
        # keep session alive to receive ExecReports
        if times:
            mean_ms = statistics.mean(times) # The arithmetic mean (average) latency per order send
            mid_ms  = statistics.median(times) # The median latency — the midpoint when all times are sorted
            max_ms  = max(times) # The maximum (worst case) latency in the batch. Formula: max_ms = max (𝑡1,𝑡2,…,𝑡𝑛) max_ms=max(t1,t2,…,tn)
            n       = len(times) # The sample size, i.e., how many orders were measured.

            cpu_secs   = sum(times) / 1000.0 #Total CPU time spent sending messages, derived from the per-iteration durations stored in times.
            wall_secs  = time.perf_counter() - run_start # The real-world elapsed time (wall clock) from the start of the run until the loop finishes.
            cpu_qps    = n / cpu_secs if cpu_secs > 0 else 0.0 # “Quotes per second” based on summed per-message durations e.g.: cpu_qps=(n/cpu_secs)
            wall_qps   = n / wall_secs if wall_secs > 0 else 0.0 # “Quotes per second” based on actual runtime (wall clock) e.g.: wall_qps= (n/wall_secs)
            
            print(f"[TIMER] mean={mean_ms:.3f} ms  median={mid_ms:.3f} ms  "
                f"max={max_ms:.3f} ms  n={n}")
            print(f"[THROUGHPUT] cpu_time={cpu_secs:.3f}s  cpu_qps={cpu_qps:.1f}  "
                f"wall_time={wall_secs:.3f}s  wall_qps={wall_qps:.1f}")
            r3wall_end_secs = datetime.now()
            r3elapsed_secs = (r3wall_end_secs - r3wall_start_secs).total_seconds()
            print(f"[Ronnie Math] quote per second: {maxloop/r3elapsed_secs:.2f}")
            print("[Ronnie Math] quote per second:", f"{maxloop / r3elapsed_secs:.2f}")            
            while True:
                time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        # ALWAYS stop initiator cleanly to avoid segfaults at interpreter shutdown
        try:
            init.stop()
        except Exception as e:
            print(f"[WARN] init.stop() raised: {e}", file=sys.stderr)
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 MasterSendOrders.RPVersion.py <initiator.cfg> [mode]")
        sys.exit(1)

    cfg = sys.argv[1]
    trademode = sys.argv[2].lower() if len(sys.argv) >= 3 else ""
    # normalize common aliases
    if trademode == "simplerepeat":
        trademode = "simplerepeat"
    elif trademode == "latency":
        trademode = "latency"
    elif trademode.startswith("layer"):
        trademode = "layerxxxx"

# SINGLE entrypoint call - do not call main() again 
main(cfg, trademode)