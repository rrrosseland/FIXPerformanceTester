#!/usr/bin/env python3
import time, uuid, threading, re
from pathlib import Path
# rpdir can start as a string or a Path—make it a Path either way
QTY           = 1
maxloop = 4
PRICE   = 0.01
incr = 0.01
################################
#trademode = "layerLower45s"
trademode = "latencyTest1"
#trademode = "repeatOrders"

if trademode == "layerLower45s":
    PRICE   = 0.01
    QTY = 1
    maxloop = 47
    incr = 0.01
if trademode == "latencyTest1":
    PRICE   = 0.02
    QTY = 1
    maxloop = 4000
    incr = 0.0
if trademode == "repeatOrders":
    PRICE   = 0.51
    QTY = 1
    maxloop = 40
    incr = 0.0

rpdir = Path("/home/ec2-user/pythonQF")          # or Path.home(), Path.cwd(), etc.
# Build child paths
data_dir = rpdir / "data"
log_file = rpdir / "logs" / "app.log"
rplog_file = rpdir / "log" / "rpapp.log"
import quickfix as fix
import quickfix50sp2 as fix50sp2
#tif=TimeInForce_GTC
tif="TimeInForce_DAY"  #### not being used as the variable would not pass
SENDER_SUB_ID = "4C001"   # EP3 user/trader (tag 50) – header on app messages only
ACCOUNT       = "YOUR_ACCOUNT"
#SYMBOL        = "CBBTC_123125_65000"
#SYMBOL        = "CBBTC_123125_142500"
SYMBOL        = "CBBTC_123125_132500"
SecSubType    = "YES"
SIDE_BUY      = True          # set False for sell

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
        #print("[APP]", msg.toString())
        # Open file in append mode
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(msg.toString() + "\n")

# Make sure directories exist (optional)
#data_dir.mkdir(parents=True, exist_ok=True)
#log_file.parent.mkdir(parents=True, exist_ok=True)

    # send one GTC limit order
    def send_gtc_limit(self, symbol, buy, qty, price, SecSubType, account=None):
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
        nos.setField(fix.SecuritySubType(SecSubType))                  # 762 (if req
        #nos.setField(fix.TimeInForce(fix.TimeInForce_GOOD_TILL_CANCEL))    # 59=1 (GTC)
        #nos.setField(fix.TimeInForce(fix.(tif.toString())))    # DAY 59=0 (DAY)
        nos.setField(fix.TimeInForce(fix.TimeInForce_DAY))    # DAY 59=0 (DAY)
        #nos.setField(fix.TimeInForce(0))    # 59=1 (GTC) and DAY = 0 THIS DOES NOT WORK!!!
        ok = fix.Session.sendToTarget(nos, self.session_id)
        #print(f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} {SecSubType} -> {ok}")
        msgstrrp = (f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} {SecSubType} -> {ok}")
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(msgstrrp + "\n")

def main(cfg):
    settings = fix.SessionSettings(cfg)
    app = App()
    store = fix.FileStoreFactory(settings)
    logs  = fix.FileLogFactory(settings)
    init  = fix.SocketInitiator(app, store, settings, logs)
    init.start()
    #### DAY ORDERS will get CXLD upon sesson logout. GTC will persist
    try:
        # wait for logon then fire one order
        while app.session_id is None: time.sleep(0.1)
        i = 1
        NEWPRICE  = PRICE 
        # layer up the lower half of book, then the upper half or start at 99 and go down in increm
        while i <= maxloop :
            NEWPRICE  =  NEWPRICE + incr
            if trademode == "latencyTest1":
                SecSubType = "YES"
                app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
            elif trademode == "layerLower45s":
                SecSubType = "YES"
                app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
                SecSubType = "NO"
                app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
            else:
                SecSubType = "YES"
                app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
                SecSubType = "NO"
                app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
            time.sleep(0.00001)
            print(i)
            i += 1
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

