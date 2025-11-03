#!/usr/bin/env python3
import time, uuid, sys, quickfix as fix, quickfix50sp2 as fix50sp2
from pathlib import Path

# -------- Settings you’ll pass or edit at top --------
Symbol    = "CBBTC_123125_142500"
SecSubType= "YES"     # or "NO"

class App(fix.Application):
    def __init__(self):
        super().__init__()
        self.Session = None

    def onCreate(self, sid): print(f"[onCreate] {sid}")
    def onLogon(self, sid):
        self.Session = sid
        print(f"[onLogon] {sid}")
        # fire initial subscribe once logged on
        self.SendMDSubscribe(Symbol, SecSubType, Depth=1, WantTrade=True, Incremental=True)

    def onLogout(self, sid): print(f"[onLogout] {sid}")

    def toAdmin(self, msg, sid):
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_Logon:
            # venue-common FIXT tags
            msg.setField(fix.EncryptMethod(0))
            msg.setField(fix.HeartBtInt(30))
            msg.setField(fix.DefaultApplVerID("9"))
            msg.setField(fix.ResetSeqNumFlag(True))
        print(f"[OUT ADMIN] {msg.toString()}")

    def fromAdmin(self, msg, sid):
        print(f"[IN ADMIN]  {msg.toString()}")

    def toApp(self, msg, sid):
        print(f"[OUT APP]   {msg.toString()}")

    def fromApp(self, msg, sid):
        raw = msg.toString()
        print(f"[IN APP]    {raw}")
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        mtype = mt.getValue()

        # Snapshot (35=W)
        if mtype == fix.MsgType_MarketDataSnapshotFullRefresh:
            # pull 55 for visibility
            sym = ""
            try: s = fix.Symbol(); msg.getField(s); sym = s.getValue()
            except fix.FieldNotFound: pass
            # compact top-of-book
            bids, asks, trades = [], [], []
            try:
                n = fix.NoMDEntries(); msg.getField(n)
                count = int(n.getValue())
                for i in range(1, count+1):
                    g = fix50sp2.MarketDataSnapshotFullRefresh.NoMDEntries()
                    msg.getGroup(i, g)
                    t = fix.MDEntryType(); g.getField(t)
                    px = None; sz = None
                    try: p = fix.MDEntryPx(); g.getField(p); px = float(p.getValue())
                    except fix.FieldNotFound: pass
                    try: s = fix.MDEntrySize(); g.getField(s); sz = float(s.getValue())
                    except fix.FieldNotFound: pass
                    if t.getValue() == '0': bids.append((px, sz))
                    elif t.getValue() == '1': asks.append((px, sz))
                    elif t.getValue() == '2': trades.append((px, sz))
            except fix.FieldNotFound: pass

            def head(x): return x[0] if x else None
            print(f"[MD SNAP] 55={sym or Symbol} bid={head(bids)} ask={head(asks)} last={head(trades)}")
            return

        # Incremental (35=X)
        if mtype == fix.MsgType_MarketDataIncrementalRefresh:
            # just count entries to show activity
            cnt = 0
            try:
                n = fix.NoMDEntries(); msg.getField(n)
                cnt = int(n.getValue())
            except fix.FieldNotFound: pass
            print(f"[MD INC] n={cnt}")
            return

        # Request Reject (35=Y)
        if mtype == 'Y':
            reason = ""
            try: t = fix.Text(); msg.getField(t); reason = t.getValue()
            except fix.FieldNotFound: pass
            print(f"[MD REJ] {reason}")

    # ---- MD subscribe (263=1), 55 + 762 ----
    def SendMDSubscribe(self, Symbol, SecSubType="YES", Depth=1, WantTrade=True, Incremental=True):
        if not self.Session:
            print("[MD ERROR] not logged on yet"); return None

        MDReqID = f"MD-{uuid.uuid4()}"
        md = fix50sp2.MarketDataRequest()
        md.setField(fix.MDReqID(MDReqID))                             # 262
        md.setField(fix.SubscriptionRequestType('1'))                 # 263=1
        md.setField(fix.MarketDepth(Depth))                           # 264
        md.setField(fix.MDUpdateType(1 if Incremental else 0))        # 265
        md.setField(fix.AggregatedBook(True))                         # 266=Y

        types = [fix.MDEntryType_BID, fix.MDEntryType_OFFER]
        if WantTrade: types.append(fix.MDEntryType_TRADE)
        for et in types:
            g = fix50sp2.MarketDataRequest.NoMDEntryTypes()
            g.setField(fix.MDEntryType(et))
            md.addGroup(g)

        rel = fix50sp2.MarketDataRequest.NoRelatedSym()
        rel.setField(fix.Symbol(Symbol))                               # 55
        rel.setField(fix.SecuritySubType(SecSubType))                  # 762
        md.addGroup(rel)

        ok = fix.Session.sendToTarget(md, self.Session)
        print(f"[SEND MD SUB] 35=V 263=1 262={MDReqID} 55={Symbol} 762={SecSubType} -> {ok}")
        return MDReqID

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 market_data.py md.cfg [Symbol] [SecSubType]")
        sys.exit(1)
    cfg = sys.argv[1]
    global Symbol, SecSubType
    if len(sys.argv) >= 3: Symbol = sys.argv[2]
    if len(sys.argv) >= 4: SecSubType = sys.argv[3]

    app = App()
    settings = fix.SessionSettings(cfg)
    store = fix.FileStoreFactory(settings)
    logs  = fix.FileLogFactory(settings)
    init  = fix.SocketInitiator(app, store, settings, logs)
    init.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        try: init.stop()
        except Exception: pass

if __name__ == "__main__":
    main()