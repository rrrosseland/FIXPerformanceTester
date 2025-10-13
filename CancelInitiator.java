package perf;

// ===== Cleaned Imports =====
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import quickfix.*;
import quickfix.field.*;
import quickfix.fix50sp2.OrderCancelRequest;

public class CancelInitiator {

    // ---- Utility helpers ----
    private static final AtomicLong CL_SEQ = new AtomicLong(System.currentTimeMillis());

    private static String nextClOrdID() {
        return Long.toString(CL_SEQ.incrementAndGet());
    }

    private static TransactTime nowTT() {
        return new TransactTime(LocalDateTime.now(ZoneOffset.UTC));
    }
    
    // --- Minimal Application Stub ---
    private static final Application APP_STUB = new Application() {
        @Override public void onCreate(SessionID id) {}
        @Override public void onLogon(SessionID id) { System.out.println("Logon successful for " + id); }
        @Override public void onLogout(SessionID id) { System.out.println("Logout complete for " + id); }
        @Override public void toAdmin(Message m, SessionID id) {}
        @Override public void fromAdmin(Message m, SessionID id) {}
        @Override public void toApp(Message m, SessionID id) {}
        @Override public void fromApp(Message m, SessionID id) {}
    };

    // ------------------------------------------------------------------
    //                         MAIN EXECUTION
    // ------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        
        // Expected args: <cfg> <sender> <target> <origClOrdID> <symbol> <account> <orderID> <side>
        if (args.length < 8) {
            System.err.println("Usage: CancelInitiator <cfg> <sender> <target> <OrigClOrdID> <Symbol> <Account> <OrderID> <Side (1/2)>");
            System.exit(2);
        }

        final String cfg       = args[0];
        final String sender    = args[1];
        final String target    = args[2];
        final String origClOrd = args[3];       // 41 (U1...)
        final String symbol    = args[4];       // 48 / 55
        final String account   = args[5];       // 1
        final String orderId   = args[6];       // 37
        final String sideIn    = args[7];       // 54 (1 or 2)
        
        // Dynamic Side Parsing (Cleaned)
        final char sideChar = "1".equals(sideIn) ? Side.BUY : Side.SELL;

        // QFJ Plumbing Setup
        SessionSettings settings = new SessionSettings(cfg);
        LogFactory fileLog   = new FileLogFactory(settings);
        LogFactory screenLog = new ScreenLogFactory(true, true, true, true);
        LogFactory logFactory= new CompositeLogFactory(new LogFactory[]{ fileLog, screenLog });
        MessageStoreFactory storeFactory = new MemoryStoreFactory();
        MessageFactory msgFactory = new DefaultMessageFactory();

        Initiator initiator = new SocketInitiator(
            APP_STUB, storeFactory, settings, logFactory, msgFactory
        );

        initiator.start();

        // Resolve and wait for logon (Wait up to 10 seconds)
        SessionID sid = new SessionID("FIXT.1.1", sender, target);
        long deadline = System.currentTimeMillis() + 10_000;
        
        while (!Session.doesSessionExist(sid) || !Session.lookupSession(sid).isLoggedOn()) {
            if (System.currentTimeMillis() > deadline) throw new RuntimeException("Logon timeout for " + sid);
            Thread.sleep(200);
        }

        // ---- Build ROBUST Order Cancel Request (35=F) ----
        OrderCancelRequest cxl = new OrderCancelRequest(
            new ClOrdID(nextClOrdID()),         // 11: New cancel ID
            new Side(sideChar),                 // 54: Original Side
            nowTT()                             // 60: TransactTime
        );
        cxl.set(new OrigClOrdID(origClOrd));     // 41: Original ClOrdID
        cxl.set(new OrderID(orderId));          // 37: Venue OrderID

        // Instrument identification (Standard required fields)
        cxl.set(new SecurityID(symbol));         // 48
        cxl.set(new Symbol(symbol));             // 55
        cxl.set(new SecurityIDSource("8"));      // 22: Exchange Symbol

        // Robust fields based on successful Replace (Ingrid/IB)
        cxl.set(new Account(account));           // 1
        cxl.set(new SecuritySubType("YES"));     // 762 (Assuming YES market variant)
        cxl.set(new AccountType(AccountType.HOUSE_TRADER));       // 581 (Assuming House Trader)

        // --- SEND AND LOG ---
        boolean ok = Session.sendToTarget(cxl, sid);
        System.out.println("Sent Cancel for " + origClOrd + " (OrderID: " + orderId + ") => " + (ok ? "SUCCESS" : "FAIL"));

        // Wait briefly for the response to be sent back before initiating stop
        Thread.sleep(500); 
        
        // Cleanly stop the initiator (will send 35=5 Logout)
        initiator.stop();
        
        // Wait for the initiator thread to completely finish before JVM exits
        Thread.sleep(500);
    }
}