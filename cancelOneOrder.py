#!/usr/bin/env python3
import sys, time, uuid, threading, re
import quickfix as fix
import quickfix50sp2 as fix50sp2

#python3 cancelOneOrder.py config/initiator.V2.cfg 'CL-9eb6d30e-72be-429f-8a99-be38998a269a' 'CBBTC_123125_125000' 'BUY'

SENDER_SUB_ID = "YOUR_USER"   # tag 50: required on application messages; NOT on Logon
WAIT_SECS = 10

SOH = "\x01"
def mask(msg: fix.Message) -> str:
    s = msg.toString()
    s = re.sub(r'(\x01553=)[^\x01]*', r'\1***', s)  # redact Username
    s = re.sub(r'(\x01554=)[^\x01]*', r'\1***', s)  # redact Password
    return s.replace(SOH, "|")

class App(fix.Application):
    def __init__(self, orig_cl, symbol, side):
        super().__init__()
        self.session_id = None
        self.orig_cl = orig_cl
        self.symbol = symbol
        self.side = side
        self.cancel_cl = None
        self.done = threading.Event()

    # lifecycle
    def onCreate(self, sid): pass
    def onLogon(self, sid):
        print("[LOGON]", sid)
        self.session_id = sid
        self.send_cancel()

    def onLogout(self, sid): print("[LOGOUT]", sid)

    # admin
    def toAdmin(self, msg, sid):
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_Logon:
            # EP3 logon: no SenderSubID (50); include 98/108/1137/141
            try: msg.getHeader().removeField(50)
            except fix.FieldNotFound: pass
            msg.setField(fix.EncryptMethod(0))       # 98=0
            msg.setField(fix.HeartBtInt(30))         # 108=30
            msg.setField(fix.DefaultApplVerID("9"))  # 1137=9 (FIX50SP2)
            msg.setField(fix.ResetSeqNumFlag(True))  # 141=Y
        print("[toAdmin] ", mask(msg))

    def fromAdmin(self, msg, sid):
        print("[fromAdmin]", mask(msg))

    # app
    def toApp(self, msg, sid):
        # EP3: tag 50 required on *application* messages
        msg.getHeader().setField(fix.SenderSubID(SENDER_SUB_ID))
        print("[toApp]    ", mask(msg))

    def fromApp(self, msg, sid):
        print("[fromApp]  ", mask(msg))
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_ExecutionReport:
            # success path: ExecType(150)=4 / OrdStatus(39)=4
            et = fix.ExecType(); os = fix.OrdStatus()
            if msg.isSetField(et): msg.getField(et)
            if msg.isSetField(os): msg.getField(os)
            if et.getValue() == fix.ExecType_CANCELLED or os.getValue() == fix.OrdStatus_CANCELED:
                print("[OK] Order canceled")
                self.done.set()
        elif mt.getValue() == fix.MsgType_OrderCancelReject:
            # show reason 102 and text 58
            reason = fix.CxlRejReason(); text = fix.Text()
            if msg.isSetField(reason): msg.getField(reason)
            if msg.isSetField(text):   msg.getField(text)
            print(f"[CXL REJECT] reason={reason.getValue() if reason else '?'} text={text.getValue() if text else ''}")
            self.done.set()
        elif mt.getValue() == fix.MsgType_Reject or mt.getValue() == fix.MsgType_BusinessMessageReject:
            self.done.set()

    # ---- send cancel ----
    def send_cancel(self):
        self.cancel_cl = "CXL-" + str(uuid.uuid4())
        m = fix50sp2.OrderCancelRequest()
        m.setField(fix.ClOrdID(self.cancel_cl))          # 11 new ID for the cancel
        m.setField(fix.OrigClOrdID(self.orig_cl))        # 41 last ClOrdID of the order
        m.setField(fix.Symbol(self.symbol))              # 55 instrument symbol
        m.setField(fix.Side(fix.Side_BUY if self.side.upper()=="BUY" else fix.Side_SELL))  # 54
        m.setField(fix.TransactTime())                   # 60 (not listed as required, but safe to send)
        ok = fix.Session.sendToTarget(m, self.session_id)
        print(f"[SEND] Cancel for OrigClOrdID={self.orig_cl} as ClOrdID={self.cancel_cl} -> {ok}")

def main(cfg, orig_cl, symbol, side):
    settings = fix.SessionSettings(cfg)
    app = App(orig_cl, symbol, side)
    store = fix.FileStoreFactory(settings)
    try:
        logs = fix.CompositeLogFactory(fix.FileLogFactory(settings), fix.ScreenLogFactory(settings))
    except AttributeError:
        logs = fix.FileLogFactory(settings)
    init = fix.SocketInitiator(app, store, settings, logs)
    init.start()
    try:
        if not app.done.wait(WAIT_SECS):
            print(f"[WARN] No response in {WAIT_SECS}s; order may already be inactive or unknown.")
        time.sleep(1)
    finally:
        init.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python ep3_cancel.py initiator.cfg <OrigClOrdID> <Symbol> <BUY|SELL>")
        sys.exit(1)
    _, cfg, orig_cl, symbol, side = sys.argv
    main(cfg, orig_cl, symbol, side)

