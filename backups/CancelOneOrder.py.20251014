#!/usr/bin/env python3
# CancelOneOrder.py
# Send exactly one Order Cancel Request (MsgType=F) for a given OrigClOrdID.

import argparse, time, uuid
from pathlib import Path
import quickfix as fix
import quickfix50sp2 as fix50sp2

# ---- Paths & logging (kept consistent with your sender) ----
rpdir = Path("/home/ec2-user/pythonQF")
rplog_file = rpdir / "log" / "rpapp.log"

SENDER_SUB_ID = "4C001"   # tag 50 (EP3 user/trader) – header on app messages only

def gen_clordid(prefix="CXL"):
    return f"{prefix}-{uuid.uuid4()}"

def parse_side(s):
    s = s.strip().lower()
    if s in ("1","buy","b"):  return fix.Side_BUY
    if s in ("2","sell","s"): return fix.Side_SELL
    raise ValueError("side must be 'buy'/'sell' or 1/2")

class App(fix.Application):
    def __init__(self, args):
        super().__init__()
        self.args = args
        self.session_id = None
        self.sent = False
        self.done = False

    # ---- lifecycle ----
    def onCreate(self, sid): pass

    def onLogon(self, sid):
        print("[LOGON]", sid)
        self.session_id = sid
        if not self.sent:
            self.send_cancel()
            self.sent = True

    def onLogout(self, sid):
        print("[LOGOUT]", sid)

    # ---- admin plumbing ----
    def toAdmin(self, msg, sid):
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_Logon:
            # Remove SenderSubID(50) from Logon; set EP3-required header/body fields
            try: msg.getHeader().removeField(50)
            except fix.FieldNotFound: pass
            msg.setField(fix.EncryptMethod(0))       # 98=0 (None)
            msg.setField(fix.HeartBtInt(30))         # 108=30
            msg.setField(fix.DefaultApplVerID("9"))  # 1137=9 (FIX50SP2)
            msg.setField(fix.ResetSeqNumFlag(True))  # 141=Y

    def fromAdmin(self, msg, sid): pass

    # ---- app plumbing ----
    def toApp(self, msg, sid):
        # Add SenderSubID(50) to *application* messages
        msg.getHeader().setField(fix.SenderSubID(SENDER_SUB_ID))

    def fromApp(self, msg, sid):
        print("[APP]", msg.toString())
        try:
            with rplog_file.open("a", encoding="utf-8") as f:
                f.write(msg.toString() + "\n")
        except Exception as e:
            print(f"[WARN] Failed to write app log: {e}")

    # ---- build & send Order Cancel Request ----
    def send_cancel(self):
        # FIX 5.0 SP2 OrderCancelRequest requires:
        # OrigClOrdID(41), ClOrdID(11), Symbol(55), Side(54), TransactTime(60)
        msg = fix50sp2.OrderCancelRequest()

        clordid = self.args.clordid or gen_clordid()
        msg.setField(fix.OrigClOrdID(self.args.orig_clordid))   # 41
        msg.setField(fix.ClOrdID(clordid))                      # 11
        msg.setField(fix.Symbol(self.args.symbol))              # 55
        msg.setField(fix.Side(parse_side(self.args.side)))      # 54
        msg.setField(fix.TransactTime())                        # 60 (now, UTC)

        # Common extras (only if your venue requires/accepts them)
        if self.args.account:
            msg.setField(fix.Account(self.args.account))        # 1
        if self.args.orderid:
            msg.setField(fix.OrderID(self.args.orderid))        # 37
        if self.args.security_subtype:
            msg.setField(fix.SecuritySubType(self.args.security_subtype))  # 762
        if self.args.security_id:
            msg.setField(fix.SecurityID(self.args.security_id))            # 48
        if self.args.security_id_source:
            msg.setField(fix.SecurityIDSource(self.args.security_id_source))# 22

        print("[SEND] OrderCancelRequest:", msg.toString())
        try:
            ok = fix.Session.sendToTarget(msg, self.session_id)
            print(f"[SEND->OK={ok}] OrigClOrdID={self.args.orig_clordid} NewClOrdID={clordid}")
            with rplog_file.open("a", encoding="utf-8") as f:
                f.write(f"[CXL] OrigClOrdID={self.args.orig_clordid} NewClOrdID={clordid} OK={ok}\n")
        except fix.SessionNotFound as e:
            print(f"ERROR: Session not found: {e}")
        finally:
            # Give a moment for cancel response(s) to arrive, then mark done
            # (main() will stop the initiator once done is True)
            time.sleep(self.args.hold_seconds)
            self.done = True

def main():
    ap = argparse.ArgumentParser(description="Send a single FIX 5.0 SP2 Order Cancel Request (MsgType=F).")
    ap.add_argument("config", help="path to initiator cfg (e.g., config/initiator.V2.cfg)")
    ap.add_argument("--orig-clordid", required=True, help="OrigClOrdID(41) of the live order to cancel")
    ap.add_argument("--symbol",       required=True, help="Symbol(55) (must match original)")
    ap.add_argument("--side",         required=True, help="buy/sell or 1/2 (must match original)")
    ap.add_argument("--clordid",      help="new cancel ClOrdID(11); default is auto-generated")
    ap.add_argument("--orderid",      help="OrderID(37) if the venue requires it")
    ap.add_argument("--account",      help="Account(1) if required by venue")
    ap.add_argument("--security-subtype", help="SecuritySubType(762) if you used it on the NOS")
    ap.add_argument("--security-id",       help="SecurityID(48) (optional)")
    ap.add_argument("--security-id-source", help="SecurityIDSource(22) e.g. 8=ExchangeSymbol, 4=ISIN")
    ap.add_argument("--hold-seconds", type=float, default=3.0,
                    help="seconds to keep session up after sending (collect responses)")
    args = ap.parse_args()

    settings = fix.SessionSettings(args.config)
    store    = fix.FileStoreFactory(settings)
    logs     = fix.FileLogFactory(settings)
    app      = App(args)
    init     = fix.SocketInitiator(app, store, settings, logs)

    init.start()
    try:
        # Wait for logon
        while app.session_id is None:
            time.sleep(0.05)
        # Wait until cancel is sent and grace period expires
        while not app.done:
            time.sleep(0.05)
    finally:
        init.stop()

if __name__ == "__main__":
    main()


