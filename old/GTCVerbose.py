#!/usr/bin/env python3
import time, uuid, threading, re
import quickfix as fix
import quickfix50sp2 as fix50sp2

# ---- configure these ----
SENDER_SUB_ID = "YOUR_USER"   # tag 50 added to app messages only
ACCOUNT       = "YOUR_ACCOUNT"
SYMBOL        = "GOOG"
SIDE_BUY      = True
QTY           = 100
PRICE         = 50.00
WAIT_SECS     = 10

SOH = "\x01"
def mask(msg: fix.Message) -> str:
    s = msg.toString()
    s = re.sub(r'(\x01553=)[^\x01]*', r'\1***', s)  # Username
    s = re.sub(r'(\x01554=)[^\x01]*', r'\1***', s)  # Password
    return s.replace(SOH, "|")

class App(fix.Application):
    def __init__(self):
        super().__init__()
        self.session_id = None
        self.last_clordid = None
        self.er_event = threading.Event()
        self.last_inbound = None

    # lifecycle
    def onCreate(self, sid): pass
    def onLogon(self, sid):
        print("[LOGON]", sid)
        self.session_id = sid
    def onLogout(self, sid): print("[LOGOUT]", sid)

    # admin
    def toAdmin(self, msg, sid):
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_Logon:
            # EP3-style logon extras; and ensure no 50 on logon
            try: msg.getHeader().removeField(50)
            except fix.FieldNotFound: pass
            msg.setField(fix.EncryptMethod(0))
            msg.setField(fix.HeartBtInt(30))
            msg.setField(fix.DefaultApplVerID("9"))
            msg.setField(fix.ResetSeqNumFlag(True))
        print("[toAdmin] ", mask(msg))
    def fromAdmin(self, msg, sid):
        print("[fromAdmin]", mask(msg))

    # app
    def toApp(self, msg, sid):
        msg.getHeader().setField(fix.SenderSubID(SENDER_SUB_ID))  # tag 50 on application msgs
        print("[toApp]    ", mask(msg))
    def fromApp(self, msg, sid):
        self.last_inbound = msg.toString()
        print("[fromApp]  ", mask(msg))
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() in (fix.MsgType_ExecutionReport, fix.MsgType_Reject, fix.MsgType_BusinessMessageReject):
            # If it matches our ClOrdID, release the wait
            cl = fix.ClOrdID()
            if msg.isSetField(cl):
                msg.getField(cl)
                if cl.getValue() == self.last_clordid:
                    self.er_event.set()

    # -------- order & status helpers --------
    def send_gtc_limit(self, symbol, buy, qty, price, account=None):
        nos = fix50sp2.NewOrderSingle()
        cl = "CL-" + str(uuid.uuid4())
        self.last_clordid = cl
        nos.setField(fix.ClOrdID(cl))
        if account: nos.setField(fix.Account(account))
        nos.setField(fix.Symbol(symbol))
        nos.setField(fix.Side(fix.Side_BUY if buy else fix.Side_SELL))
        nos.setField(fix.TransactTime())
        nos.setField(fix.OrdType(fix.OrdType_LIMIT))
        nos.setField(fix.OrderQty(float(qty)))
        nos.setField(fix.Price(float(price)))
        nos.setField(fix.TimeInForce(fix.TimeInForce_GOOD_TILL_CANCEL))  # 59=1 GTC
        ok = fix.Session.sendToTarget(nos, self.session_id)
        print(f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} -> {ok} ClOrdID={cl}")
        return cl

    def send_order_status_request(self):
        if not self.last_clordid: return
        req = fix50sp2.OrderStatusRequest()
        req.setField(fix.ClOrdID(self.last_clordid))   # 11 (same clordid)
        req.setField(fix.Symbol(SYMBOL))               # 55
        req.setField(fix.Side(fix.Side_BUY if SIDE_BUY else fix.Side_SELL))  # 54
        fix.Session.sendToTarget(req, self.session_id)
        print("[SEND] OrderStatusRequest for", self.last_clordid)

def main(cfg):
    settings = fix.SessionSettings(cfg)
    app = App()

    store = fix.FileStoreFactory(settings)
    # File + Screen logs (very useful)
    try:
        logs = fix.CompositeLogFactory(fix.FileLogFactory(settings), fix.ScreenLogFactory(settings))
    except AttributeError:
        logs = fix.FileLogFactory(settings)

    init = fix.SocketInitiator(app, store, settings, logs)
    init.start()
    try:
        # wait for logon
        t0 = time.time()
        while app.session_id is None:
            if time.time() - t0 > 15: raise RuntimeError("Logon timeout")
            time.sleep(0.1)

        app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, PRICE, ACCOUNT)

        # wait for ExecReport/Reject on that ClOrdID
        if not app.er_event.wait(WAIT_SECS):
            print(f"[WARN] No ExecReport within {WAIT_SECS}s; sending OrderStatusRequest…")
            app.send_order_status_request()
            if not app.er_event.wait(WAIT_SECS):
                print(f"[WARN] Still no response after {2*WAIT_SECS}s. Check venue requirements and logs.")
        else:
            print("[OK] ExecutionReport/Reject received for", app.last_clordid)

        # brief grace period for any final messages, then exit
        time.sleep(2)
    finally:
        init.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv)!=2:
        print("Usage: python ep3_limit_gtc_verbose.py initiator.cfg"); raise SystemExit(1)
    main(sys.argv[1])

