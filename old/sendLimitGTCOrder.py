#!/usr/bin/env python3
import time, uuid, threading, re

import quickfix as fix
import quickfix50sp2 as fix50sp2

SENDER_SUB_ID = "4C001"   # EP3 user/trader (tag 50) – header on app messages only
ACCOUNT       = "YOUR_ACCOUNT"
SYMBOL        = "CBBTC_123125_125000"
SecSubType    = "YES"
SIDE_BUY      = True          # set False for sell
QTY           = 1
PRICE         = 0.12

class App(fix.Application):
    def __init__(self):
        super().__init__()
        self.session_id = None
        self.sent = False

    # lifecycle
    def onCreate(self, sid): pass
    def onLogon(self, sid):
        print("[LOGON]", sid)
        self.session_id = sid
    def onLogout(self, sid): print("[LOGOUT]", sid)

    # admin plumbing
    def toAdmin(self, msg, sid):
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_Logon:
            # Ensure no SenderSubID(50) on Logon; set basic EP3 fields
            try: msg.getHeader().removeField(50)
            except fix.FieldNotFound: pass
            msg.setField(fix.EncryptMethod(0))     # 98=0
            msg.setField(fix.HeartBtInt(30))       # 108=30
            msg.setField(fix.DefaultApplVerID("9"))# 1137=9 (FIX50SP2)
            msg.setField(fix.ResetSeqNumFlag(True))# 141=Y
    def fromAdmin(self, msg, sid): pass

    # app plumbing
    def toApp(self, msg, sid):
        # Add SenderSubID(50) to *all* application messages
        msg.getHeader().setField(fix.SenderSubID(SENDER_SUB_ID))
    def fromApp(self, msg, sid):
        print("[APP]", msg.toString())

    # send one GTC limit order
    def send_gtc_limit(self, symbol, buy, qty, price, account=None):
        nos = fix50sp2.NewOrderSingle()
        nos.setField(fix.ClOrdID("CL-" + str(uuid.uuid4())))              # 11
        if account: nos.setField(fix.Account(account))                     # 1
        nos.setField(fix.Symbol(symbol))                                   # 55
        nos.setField(fix.Side(fix.Side_BUY if buy else fix.Side_SELL))     # 54
        nos.setField(fix.TransactTime())                                   # 60 (now)
        nos.setField(fix.OrdType(fix.OrdType_LIMIT))                       # 40=2
        nos.setField(fix.OrderQty(float(qty)))                             # 38
        nos.setField(fix.Price(float(price)))                              # 44
        nos.setField(fix.CustOrderCapacity(1))                             # 582 = 5 (RETAIL
        nos.setField(fix.AccountType(1))                  # 581 (if req
        nos.setField(fix.SecuritySubType("YES"))                  # 762 (if req
        nos.setField(fix.TimeInForce(fix.TimeInForce_GOOD_TILL_CANCEL))    # 59=1 (GTC)
        ok = fix.Session.sendToTarget(nos, self.session_id)
        print(f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} -> {ok}")

def main(cfg):
    settings = fix.SessionSettings(cfg)
    app = App()
    store = fix.FileStoreFactory(settings)
    logs  = fix.FileLogFactory(settings)
    init  = fix.SocketInitiator(app, store, settings, logs)
    init.start()
    try:
        # wait for logon then fire one order
        while app.session_id is None: time.sleep(0.1)
        app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, PRICE, ACCOUNT)
        # keep session alive to receive ExecReports
        while True: time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        init.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv)!=2:
        print("Usage: python ep3_limit_gtc.py initiator.cfg"); raise SystemExit(1)
    main(sys.argv[1])

