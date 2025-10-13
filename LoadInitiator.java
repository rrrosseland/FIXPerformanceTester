package perf;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Member;
import java.math.BigDecimal;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import quickfix.Application;
import quickfix.CompositeLogFactory;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.DoNotSend;
import quickfix.FieldNotFound;
import quickfix.FileLogFactory;
import quickfix.FileStoreFactory;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.Initiator;
import quickfix.LogFactory;
import quickfix.MemoryStoreFactory;
import quickfix.Message;
import quickfix.MessageCracker;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.ScreenLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.UnsupportedMessageType;
import quickfix.field.Account;
import quickfix.field.AccountType;
import quickfix.field.ClOrdID;
import quickfix.field.Currency;
import quickfix.field.CustOrderCapacity;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.HandlInst;
import quickfix.field.HeartBtInt;
import quickfix.field.LeavesQty;
import quickfix.field.MsgType;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderCapacity;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.OrigClOrdID;
import quickfix.field.Password;
import quickfix.field.Price;
import quickfix.field.RefMsgType;
import quickfix.field.RefTagID;
import quickfix.field.ResetSeqNumFlag;
import quickfix.field.SecurityID;
import quickfix.field.SecurityIDSource;
import quickfix.field.SecuritySubType;
import quickfix.field.SecurityType;
import quickfix.field.SessionRejectReason;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.Text;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.field.Username;
import quickfix.fix50sp2.ExecutionReport;
import quickfix.fix50sp2.NewOrderSingle;
import quickfix.fix50sp2.OrderCancelReplaceRequest;
import quickfix.fix50sp2.OrderCancelRequest;

public class LoadInitiator {

        // ---------- CLI params ----------
        static class CliParams {
            String cfg;
            String yesno;
            BigDecimal price;
            String symbol;
            String accountname;
            Integer orderquantity;
        }

        private static CliParams parseArgs(String[] args) {
            
            CliParams p = new CliParams();
            p.cfg = args[0];

            for (int i = 1; i < args.length; i++) {
                String a = args[i];
                int eq = a.indexOf('=');
                if (eq <= 0 || eq == a.length() - 1) continue;
                String key = a.substring(0, eq).trim().toLowerCase(Locale.ROOT);
                String val = a.substring(eq + 1).trim();
                switch (key) {
                    case "yesno":         p.yesno = val; break;
                    case "price":         p.price = new java.math.BigDecimal(val); break;
                    case "symbol":        p.symbol = val; break;
                    case "accountname":   p.accountname = val; break;
                    case "orderquantity": p.orderquantity = Integer.parseInt(val); break;
                    default: /* ignore */ ;
                }
            }
            return p;
        }
    
    //Keep a visible banner/version and bump it whenever you change code:
    private static final String BANNER = "[BANNER v8] submit-only, minimal fields";
    // ---------- main orchestrator ----------
    public static void main(String[] args) {
        System.out.println("[WHERE] Class loaded from: " +
            LoadInitiator.class.getProtectionDomain().getCodeSource().getLocation());
        System.out.println("[WHERE] CWD=" + System.getProperty("user.dir"));
        System.out.println("[BANNER v8] submit-only, minimal fields");
        Initiator initiator = null;
        App app = null;
        try {
            CliParams P = parseArgs(args);
            System.out.println("[ARGS] len=" + args.length + " -> " + java.util.Arrays.toString(args));
            System.out.println("[ARGS] yesno=" + P.yesno);
            System.out.println("[CHK1] cfg=" + P.cfg + " exists=" + Files.exists(Paths.get(P.cfg)));

            SessionSettings settings = new SessionSettings(P.cfg);

            try {
                String useDD  = settings.isSetting("UseDataDictionary")
                    ? settings.getString("UseDataDictionary") : "<unset>";
                String appDD  = settings.isSetting("AppDataDictionary")
                    ? settings.getString("AppDataDictionary") : "<unset>";
                System.out.println("[CFG] UseDataDictionary=" + useDD);
                System.out.println("[CFG] AppDataDictionary=" + appDD);
            } catch (quickfix.ConfigError ce) {
                System.out.println("[CFG] (error reading settings): " + ce.getMessage());
            } 

            app = new App();
            app.setCli(P.accountname, P.symbol, P.orderquantity, P.price, P.yesno);

            MessageStoreFactory store = new MemoryStoreFactory();
            MessageFactory mf = new DefaultMessageFactory();
            LogFactory logFactory = new CompositeLogFactory(new LogFactory[]{
                new FileLogFactory(settings),
                new ScreenLogFactory(true, true, true, true)
            });

            initiator = new SocketInitiator(app, store, settings, logFactory, mf);
            System.out.println("[CHK2] initiator.start()");
            initiator.start();
            Thread.sleep(3000);

            SessionID id = null;
            for (SessionID sid : initiator.getSessions()) {
                Session s = Session.lookupSession(sid);
                if (s != null && s.isLoggedOn()) { id = sid; break; }
            }
            if (id == null) { System.err.println("[ERR] no logon in window"); return; }

            app.sendNOS(id);

        } catch (Throwable t) {
            t.printStackTrace(System.err);
        } finally {
            if (app != null) app.shutdown();
            if (initiator != null) initiator.stop();
        }
    }

    // ---------- FIX application ----------
    static class App extends quickfix.fix50sp2.MessageCracker implements Application {
        private final ScheduledExecutorService hbScheduler = Executors.newSingleThreadScheduledExecutor(r -> { var t = new Thread(r, "hb"); t.setDaemon(true); return t; });
        private ScheduledFuture<?> hbTask;
        private final int heartBtIntSeconds = 30;
        private volatile SessionID currentSession;       

        // In class App (fields)
        private String  cliAccount;
        private String  cliSymbol;
        private Integer cliQty;
        private BigDecimal cliPrice;
        private String  cliYesNo;

        // Template & IDs
        private final NewOrderSingle template;
        private final ClOrdIdGen clgen;

        // Replace your current setter with this:
        public void setCli(String account, String symbol, Integer qty, BigDecimal price, String yesno) {
            this.cliAccount = account;
            this.cliSymbol  = symbol;
            this.cliQty     = qty;
            this.cliPrice   = price;
            this.cliYesNo   = (yesno == null ? null : yesno.toUpperCase(Locale.ROOT));
        }
       
        private static long loadCounterFromDisk() {
            Path p = Paths.get("data/clseq.dat");
            try { 
                if (Files.exists(p)) return Long.parseLong(Files.readString(p).trim()); 
            } catch (Exception ignore) {}
            return 0L;
        }
        private static void saveCounterToDisk(long v) {
            try { 
                Files.createDirectories(Paths.get("data")); 
                Files.writeString(Paths.get("data/clseq.dat"), Long.toString(v)); 
            } catch (IOException ignore) {}
        }

        App() {
            template = new NewOrderSingle();
            template.setField(new OrdType(OrdType.LIMIT));
            template.setField(new TimeInForce(TimeInForce.GOOD_TILL_CANCEL));
            template.setField(new Currency("USD"));
            template.setField(new Side(Side.BUY));            
            long start = loadCounterFromDisk();
            clgen = new ClOrdIdGen("U1", start);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> saveCounterToDisk(clgen.currentCounter())));
        }          

       @Override
            public void onMessage(ExecutionReport er, SessionID sessionID)
                    throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
                String clId    = er.isSetField(11) ? er.getString(11) : "?";
                char ordStatus = er.isSetField(39) ? er.getChar(39) : '?';
                String text    = er.isSetField(58) ? er.getString(58) : "";
                System.out.println("[ER] clOrdId=" + clId + " ordStatus=" + ordStatus + " text=" + text);
            }

        @Override public void onCreate(SessionID sessionID) {}

        @Override public void onLogon(SessionID sessionID) {
            this.currentSession = sessionID;
            if (hbTask != null) hbTask.cancel(false);
            hbTask = hbScheduler.scheduleAtFixedRate(() -> {
                try {
                    Session s = Session.lookupSession(this.currentSession);
                    if (s != null && s.isLoggedOn()) {
                        Message beat = new Message();
                        beat.getHeader().setString(MsgType.FIELD, MsgType.HEARTBEAT);
                        Session.sendToTarget(beat, this.currentSession);
                    }
                } catch (Exception ignored) {}
            }, heartBtIntSeconds, heartBtIntSeconds, java.util.concurrent.TimeUnit.SECONDS);
        }

        @Override public void onLogout(SessionID sessionId) { if (hbTask != null) hbTask.cancel(true); hbScheduler.shutdownNow(); this.currentSession = null; }
        @Override public void toAdmin(Message message, SessionID sessionID) { try { if (MsgType.LOGON.equals(message.getHeader().getString(MsgType.FIELD))) message.setInt(HeartBtInt.FIELD, heartBtIntSeconds);} catch (FieldNotFound ignored) {} }
        @Override public void fromAdmin(Message message, SessionID sessionID) {}
        @Override public void toApp(Message message, SessionID sessionID) throws DoNotSend {}
        //@Override public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType { crack(message, sessionID); }
        @Override
            public void fromApp(Message message, SessionID sessionID)
                    throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
                // Route messages to typed handlers
                crack(message, sessionID);
            }

        void shutdown() { if (hbTask != null) hbTask.cancel(true); hbScheduler.shutdownNow(); }

        class ClOrdIdGen {
            private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneOffset.UTC);
            private final String prefix; private final AtomicLong counter;
            ClOrdIdGen(String prefix, long start) { this.prefix = prefix; this.counter = new AtomicLong(start); }
            public String next() { String ts = TS.format(Instant.now()); long c = counter.incrementAndGet(); return prefix + ts + String.format("%04d", c); }
            public long currentCounter() { return counter.get(); }
        }

        // ---- Send NOS (only the essentials and safety checks) ----
        void sendNOS(SessionID id) {
            Session s = Session.lookupSession(id);
            
            // 1. ADD: CHECK IF SESSION IS LOGGED ON. If not, abort gracefully.
            if (s == null || !s.isLoggedOn()) {
                // You might want a system print here:
                // System.err.println("Attempted to send NOS, but session is not logged on or null.");
                return; 
            }

            NewOrderSingle msg = (NewOrderSingle) template.clone();
            String clOrdId = clgen.next();

            // Core fields
            msg.setField(new Account(cliAccount));
            msg.setField(new ClOrdID(clOrdId));
            msg.setField(new OrderQty(cliQty));
            msg.setField(new Price(cliPrice.doubleValue()));
            msg.setField(new Symbol(cliSymbol));
            msg.setField(new Currency("USD"));
            msg.setField(new Side(Side.BUY));
            msg.setField(new TimeInForce(TimeInForce.GOOD_TILL_CANCEL));
            msg.setField(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));

            // Venue-required
            msg.setField(new quickfix.field.AccountType(1));
            msg.setField(new quickfix.field.OrderCapacity('A'));
            msg.setField(new quickfix.field.CustOrderCapacity(1));
            
            // 2. KEEP: CONDITIONAL setField for Tag 762
            if (cliYesNo != null && !cliYesNo.isBlank()) {
                msg.setField(new SecuritySubType(cliYesNo));
            }
            
            // 3. ADD: REQUIRED try-catch block for SessionNotFound
            try { 
                Session.sendToTarget(msg, id); 
            } catch (quickfix.SessionNotFound e) { 
                System.err.println("Session not found during NOS send: " + e.getMessage()); 
            }
        }
        
    }
}