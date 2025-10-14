#!/usr/bin/env python3
import time, uuid, threading, re, pytz
from pathlib import Path
from datetime import datetime

# rpdir can start as a string or a Path—make it a Path either way
rpdir = Path("/home/ec2-user/pythonQF")          # or Path.home(), Path.cwd(), etc.
# Build child paths
data_dir = rpdir / "data"
log_file = rpdir / "logs" / "app.log"
rplog_file = rpdir / "logs" / "rpapp.log"
import quickfix as fix
import quickfix50sp2 as fix50sp2

#------------definition of variables
#...................................
tif            ="GOOD_TILL_CANCEL"
#tif             ="TimeInForce_DAY"

SENDER_SUB_ID  = "4C001"   # EP3 user/trader (tag 50) – header on app messages only
# ACCOUNT      = "yesRonaldo"
# ACCOUNT      = "noRonaldo"
# ACCOUNT      = "yesTippy"
ACCOUNT        = "noTippy"
#SYMBOL        = "CBBTC_123125_65000"
#SYMBOL        = "CBBTC_123125_142500"
#SYMBOL        = "MNYCG_110425_Mamdani" 
SYMBOL         = "CBBTC_123125_132500"
SecSubType     = "NO"
#SecSubType     = "YES"
SIDE_BUY       = True  # set False for sell
QTY            = 1
maxloop = 400
PRICE   = 0.51
incr = 0.00

class App(fix.Application):
    def __init__(self):
        super().__init__()
        self.session_id = None
        self.sent = False

    # lifecycle - required for QuickFix: onCreate, onLogon, onLogout, toAdmin, fromAdmin, toApp & fromApp
    def onCreate(self, sid): pass
    
    def onLogon(self, sid):
        # Get current time in UTC and EST
        utc_time = datetime.now(pytz.UTC)
        utc_str = utc_time.strftime('%H:%M:%S %Z')
        est_time = datetime.now(pytz.timezone('US/Eastern'))
        est_str = est_time.strftime('%H:%M:%S %Z')
        print(f"[LOGON] Session ID: {sid} at {est_str} or {utc_str}")
        #print("[LOGON]", sid)

        self.session_id = sid
        #t = threading.Thread(target=self._workflow, name="workflow", daemon=True)
        # t.start()
    def onLogout(self, sid):
        utc_time = datetime.now(pytz.UTC)
        est_time = datetime.now(pytz.timezone('US/Eastern'))
        utc_str = utc_time.strftime('%H:%M:%S %Z')
        est_str = est_time.strftime('%H:%M:%S %Z')
        print(f"[COMPLETION] Workflow finished at {utc_str} / {est_str}")
        # print("[LOGOUT]", Session ID: {sid} at {est_str} or {utc_str}")

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
        nos.setField(fix.TimeInForce(fix.TimeInForce_GOOD_TILL_CANCEL))    # 59=1 (GTC)
        #nos.setField(fix.TimeInForce(fix.(tif.toString())))    # DAY 59=0 (DAY)
        #nos.setField(fix.TimeInForce(fix.TimeInForce_DAY))    # DAY 59=0 (DAY)
        #nos.setField(fix.TimeInForce(0))    # 59=1 (GTC) and DAY = 0 THIS DOES NOT WORK!!!
        ok = fix.Session.sendToTarget(nos, self.session_id)
        print(f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} {SecSubType} -> {ok}")
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
            SecSubType = "YES"
            app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
            SecSubType = "NO"
            app.send_gtc_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
            time.sleep(0.01)
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