#!/usr/bin/env python3
import time, uuid, threading, re, pytz, sys, statistics, datetime
import quickfix as fix
import quickfix50sp2 as fix50sp2
from pathlib import Path
from datetime import datetime
rpdir = Path("/home/ec2-user/pythonQF")
#
print ("possible modes simpleRepeat latency layerxxxx")
print ("dont forget to have the config file as the first arg..")

cfg = sys.argv[1]
trademode = sys.argv[2]

print ("trademode")

if trademode == "simpleRepeat":
    PRICE  = 0.49
    QTY = 1
    maxloop = 10
    incr = 0.00

print("The Price is: ", PRICE)
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
SYMBOL = "CBBTC_123125_142500"
# SYMBOL = "MNYCG_110425_Mamdani" 
# SYMBOL = "CBBTC_123125_132500"
SecSubType = "YES"
#SecSubType = "NO"
SIDE_BUY = True  # set False for sell

class App(fix.Application):
    def __init__(self):
        super().__init__()
        self.session_id = None 
        self.logged_on = False
        
    def onCreate(self, sid):
        # CRITICAL: Set the session ID here upon creation
        self.session_id = sid 
        print(f"[onCreate] Session created: {sid}")
    
    def onLogon(self, sid):
        # Set logged_on to True when the Logon message is acknowledged by the server
        self.logged_on = True
        print(f"[onLogon] Logged on: {sid}")
        
    def onLogout(self, sid):
        self.logged_on = False
        print(f"[onLogout] Logged out: {sid}")
        # keep the last session_id so we can re-use on reconnect if needed to hold the logout...

    def toAdmin(self, msg, sid):
        mt = fix.MsgType(); msg.getHeader().getField(mt)
        if mt.getValue() == fix.MsgType_Logon: 
            pass

    def fromAdmin(self, msg, sid):
        pass
    
    def toApp(self, msg, sid):
        # Add SenderSubID(50) to *all* application messages
        msg.getHeader().setField(fix.SenderSubID(SENDER_SUB_ID))

    def fromApp(self, msg, sid):
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(msg.toString() + "\n")   

    def send_limit(self, symbol, buy, qty, price, SecSubType, account=None):
        nos = fix50sp2.NewOrderSingle()
        nos.setField(fix.ClOrdID("CL-" + str(uuid.uuid4())))              # 11
        if account: nos.setField(fix.Account(account))                     # 1
        nos.setField(fix.Symbol(symbol))                                   # 55
        nos.setField(fix.Side(fix.Side_BUY))     # 54
        nos.setField(fix.TransactTime())                                   # 60 (now)
        nos.setField(fix.OrdType(fix.OrdType_LIMIT))                       # 40=2
        nos.setField(fix.OrderQty(float(qty)))                             # 38
        nos.setField(fix.Price(float(price)))                              # 44
        nos.setField(fix.CustOrderCapacity(1))                             # 582 = 5 (RETAIL
        nos.setField(fix.AccountType(1))                                   # 581 
        nos.setField(fix.SecuritySubType(SecSubType))                    # 762 Required for YES NO
        #tif = TimeInForce_GOOD_TILL_CANCEL
        #tif = TimeInForce_DAY ###to move tif outside this def, need to add as a param of def ##
        #nos.setField(fix.TimeInForce(fix.TimeInForce_GOOD_TILL_CANCEL))   # 59=1 (GTC)
        #nos.setField(fix.TimeInForce(fix.tif)    # DAY 59=0 (DAY)
        nos.setField(fix.TimeInForce(fix.TimeInForce_DAY))    # DAY 59=0 (DAY)
        ok = fix.Session.sendToTarget(nos, self.session_id)
        #nos.setField(fix.TimeInForce(0))    # 59=1 (GTC) and DAY = 0 THIS DOES NOT WORK!!!
        #print(f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} {SecSubType} -> {ok}")
        msgstrrp = (f"[SEND] GTC LIMIT {symbol} {('BUY' if buy else 'SELL')} {qty} @ {price} {SecSubType} -> {ok}")
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(msgstrrp + "\n")

def main(cfg, trademode):
    settings = fix.SessionSettings(cfg)
    app = App()
    r3wall_start_secs = datetime.now()
    times = [] 
    
    # Initialization MUST be done outside the main try block
    store = fix.FileStoreFactory(settings)
    logs = fix.FileLogFactory(settings)
    init = fix.SocketInitiator(app, store, settings, logs)
    
    init.start()
    run_start = time.perf_counter()

    logon_timeout_secs = 10 
    start_time = time.time()
    
    # Use ONE main try...finally block for execution and cleanup
    try:
        # 1. Wait for session object to be created
        while app.session_id is None and (time.time() - start_time) < 5:
            time.sleep(0.1)

        if app.session_id is None:
            print("❌ ERROR: Failed to create session object. Check config file paths/permissions.")
            return

        # 2. WAIT FOR SUCCESSFUL LOGON (max 10 seconds total)
        while not app.logged_on and (time.time() - start_time) < logon_timeout_secs:
            time.sleep(0.1)

        if not app.logged_on:
            print(f"❌ ERROR: Failed to LOGON within {logon_timeout_secs} seconds. Orders NOT SENT.")
            print("Action: Check your QuickFIX logs for the server's rejection reason (Tags 49, 56, 34, etc.).")
            # If logon fails, we stop here (do not proceed to order loop)
            return 
            
        print("✅ LOGON SUCCESSFUL. Sending orders...")

        # --- TRADING LOOP STARTS HERE (Only if logon succeeded) ---
        
        i = 1
        NEWPRICE = PRICE
        
        print("before the loop here are the values:", SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)
        
        # layer up the lower half of book, then the upper half or start at 99 and go down in increm
        while i <= maxloop:
            start = time.perf_counter()
            NEWPRICE = NEWPRICE + incr
            
            # Simplified mode checking:
            # We assume one of the modes is active based on the arguments
            app.send_limit(SYMBOL, SIDE_BUY, QTY, NEWPRICE, SecSubType, ACCOUNT)

            elapsed = (time.perf_counter() - start) * 1000.0 # ms
            times.append(elapsed)

            # optional throttle / logging — safe to remove for raw throughput
            time.sleep(0.00001)
            i += 1
        
        # ... (rest of your post-loop statistics and print statements) ...
        if times:
            mean_ms = statistics.mean(times)
            mid_ms = statistics.median(times)
            max_ms = max(times)
            n = len(times)
            cpu_secs = sum(times) / 1000.0
            wall_secs = time.perf_counter() - run_start
            cpu_qps = n / cpu_secs if cpu_secs > 0 else 0.0
            wall_qps = n / wall_secs if wall_secs > 0 else 0.0         
           
        
        # keep session alive to receive ExecReports
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

# SINGLE entrypoint call; do not call main() again below
main(cfg, trademode)