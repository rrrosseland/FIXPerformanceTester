#!/usr/bin/env python3
import sys, time
import quickfix as fix

class SimpleApp(fix.Application):
    def __init__(self, settings: fix.SessionSettings):
        super().__init__()
        self.settings = settings

    # --- lifecycle callbacks ---
    def onCreate(self, sessionID): print(f"[onCreate]  {sessionID}")
    def onLogon(self, sessionID):  print(f"[onLogon]   {sessionID}")
    def onLogout(self, sessionID): print(f"[onLogout]  {sessionID}")

    # --- admin/application plumbing ---
    def toAdmin(self, message, sessionID):
        # Add Username/Password (553/554) on Logon if present in cfg
        msgType = fix.MsgType()
        message.getHeader().getField(msgType)
        if msgType.getValue() == fix.MsgType_Logon:
            dic = self.settings.get(sessionID)
            if dic.has("Username"):
                message.setField(fix.Username(dic.getString("Username")))
            if dic.has("Password"):
                message.setField(fix.Password(dic.getString("Password")))
            # If the venue asks for a clean start but doesn’t reset seq on their side:
            # message.setField(fix.ResetSeqNumFlag(True))
        print(f"[toAdmin]   {message.toString()}")

    def fromAdmin(self, message, sessionID):
        print(f"[fromAdmin] {message.toString()}")

    def toApp(self, message, sessionID):
        print(f"[toApp]     {message.toString()}")

    def fromApp(self, message, sessionID):
        print(f"[fromApp]   {message.toString()}")

def main(cfg_path):
    settings = fix.SessionSettings(cfg_path)
    app = SimpleApp(settings)
    storeFactory = fix.FileStoreFactory(settings)
    logFactory = fix.FileLogFactory(settings)
    initiator = fix.SocketInitiator(app, storeFactory, settings, logFactory)

    initiator.start()
    print("[info] Initiator started. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[info] Stopping…")
    finally:
        initiator.stop()
        print("[info] Stopped.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python simple_initiator.py initiator.cfg")
        sys.exit(1)
    main(sys.argv[1])

