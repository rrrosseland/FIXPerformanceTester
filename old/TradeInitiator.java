package perf;

import quickfix.*;
import quickfix.field.*;
import quickfix.fix50sp2.NewOrderSingle;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TradeInitiator implements Application {
    private final SessionSettings settings;
    private Initiator initiator;
    private SessionID oeSession;

    public TradeInitiator(String cfgPath) throws Exception {
        this.settings = new SessionSettings(cfgPath);
    }

    @Override public void onCreate(SessionID sessionID) {}
    @Override public void onLogout(SessionID sessionID) { System.out.println("LOGOUT " + sessionID); }
    @Override public void toAdmin(Message message, SessionID sessionID) {}
    @Override public void fromAdmin(Message message, SessionID sessionID) {}
    @Override public void toApp(Message message, SessionID sessionID) {}
    @Override public void fromApp(Message message, SessionID sessionID) { System.out.println("IN  " + message); }

    @Override
    public void onLogon(SessionID sessionID) {
        System.out.println("LOGON " + sessionID);
        oeSession = sessionID;
        try {
            sendOneOrder(1, .49, "CBBTC_123125_125000"); // single sanity order
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendOneOrder(double qty, double px, String symbol) throws SessionNotFound {
        LocalDateTime nowUtc = LocalDateTime.now(ZoneOffset.UTC);

        NewOrderSingle nos = new NewOrderSingle(
            new ClOrdID("CL" + System.currentTimeMillis()),
            new Side(Side.BUY),
            new TransactTime(nowUtc),
            new OrdType(OrdType.LIMIT)
        );
        nos.set(new OrderQty(qty));
        nos.set(new Price(px));
        nos.set(new TimeInForce(TimeInForce.DAY));
        nos.set(new HandlInst(HandlInst.AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION));
        nos.set(new SecurityType("EVENT"));
        nos.set(new SecuritySubType("YES"));
        nos.set(new AccountType(1));
        nos.set(new Account("4C12345"));
        nos.set(new CustOrderCapacity(1));
        nos.set(new Symbol(symbol));
        nos.set(new SecurityID(symbol));
        nos.set(new SecurityIDSource(SecurityIDSource.EXCHANGE_SYMBOL));


        // If EP3 requires Account:
        // nos.set(new Account("YOUR_ACCOUNT"));

        boolean ok = Session.sendToTarget(nos, oeSession);
        System.out.println("OUT " + (ok ? "sent" : "not-sent") + " " + nos);
    }


    private void start() throws Exception {
        MessageStoreFactory storeFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new FileLogFactory(settings);
            // Use the default factory so FIXT.1.1 admin messages (Logon) are created correctly
        MessageFactory msgFactory = new DefaultMessageFactory();
        initiator = new SocketInitiator(this, storeFactory, settings, logFactory, msgFactory);
        initiator.start();
}

    public static void main(String[] args) throws Exception {
        String cfg = (args.length > 0) ? args[0] : "configs/initiator_oe.cfg";
        TradeInitiator app = new TradeInitiator(cfg);
        app.start();
        Thread.sleep(Long.MAX_VALUE);
    }
}