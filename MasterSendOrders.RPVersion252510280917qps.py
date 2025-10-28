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
    PRICE  = 0.51
    QTY = 1
    maxloop = 200
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
# SYMBOL = "CBBTC_123125_142500"
# SYMBOL = "MNYCG_110425_Mamdani" 
SYMBOL = "CBBTC_123125_132500"
SecSubType = "YES"
#SecSubType = "NO"
SIDE_BUY = True  # set False for sell

class App(fix.Application):
    def __init__(self):
        super().__init__()
        self.session_id = None        
        
    def onCreate(self, sid):
        print(f"[onCreate] Session created: {sid}")
    
    def onLogon(self, sid):        
        self.session_id = sid
        self.logged_on = True
        print(f"[onLogon] Logged on: {sid}")
        
    def onLogout(self, sid):
        self.logged_on = False
        print(f"[onLogout] Logged out: {sid}")
        # keep the last session_id so we can re-use on reconnect if needed to hold the logout...

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
    
    def fromAdmin(self, msg, sid):
        pass

    # app plumbing
    def toApp(self, msg, sid):
        # Add SenderSubID(50) to *all* application messages
        msg.getHeader().setField(fix.SenderSubID(SENDER_SUB_ID))

    def fromApp(self, msg, sid):
        # print("[APP]", msg.toString())
        # Open file in append mode
        with rplog_file.open("a", encoding="utf-8") as f:
            f.write(msg.toString() + "\n")   

	# Make sure directories exist (optional)
	#data_dir.mkdir(parents=True, exist_ok=True)
	#log_file.parent.mkdir(parents=True, exist_ok=True)
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
    times = []   # <-- before your while loop chat
    store = fix.FileStoreFactory(settings)
    logs  = fix.FileLogFactory(settings)
    init  = fix.SocketInitiator(app, store, settings, logs)
    times = []
    init.start()
    run_start = time.perf_counter()

    #### DAY ORDERS will get CXLD upon sesson logout. GTC will persist
    try:
        # wait for logon then fire orders
        while app.session_id is None:
            time.sleep(0.1)
        
        i = 1
        NEWPRICE  = PRICE
       
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
            r3wall_secs = datetime.now()
            cpu_qps    = n / cpu_secs if cpu_secs > 0 else 0.0 # “Quotes per second” based on summed per-message durations e.g.: cpu_qps=(n/cpu_secs)
            wall_qps   = n / wall_secs if wall_secs > 0 else 0.0 # “Quotes per second” based on actual runtime (wall clock) e.g.: wall_qps= (n/wall_secs)

            print(f"[TIMER] mean={mean_ms:.3f} ms  median={mid_ms:.3f} ms  "
                f"max={max_ms:.3f} ms  n={n}")
            print(f"[THROUGHPUT] cpu_time={cpu_secs:.3f}s  cpu_qps={cpu_qps:.1f}  "
                f"wall_time={wall_secs:.3f}s  wall_qps={wall_qps:.1f}")
            
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