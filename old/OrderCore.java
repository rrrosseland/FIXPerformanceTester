package perf;

// ===== imports from Java and QuickFIX=====
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;

import java.math.BigDecimal;

import java.net.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Date;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import perf.LoadInitiator.App;
import quickfix.Application;
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
import quickfix.field.HeartBtInt;
import quickfix.field.HandlInst;
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

public final class OrderCore {
    private static final AtomicLong SEQ = new AtomicLong(System.currentTimeMillis()); // simple unique seed

    private OrderCore() {}

    public static String nextClOrdID() { return "C" + SEQ.incrementAndGet(); }

    public static TransactTime nowTT() { return new TransactTime(LocalDateTime.now()); }

    public static NewOrderSingle makeLimitNOS(String symbol, char side, double px, double qty) {
        NewOrderSingle nos = new NewOrderSingle(
            new ClOrdID(nextClOrdID()),
            new Side(side),
            nowTT(),
            new OrdType(OrdType.LIMIT)
        );
        nos.set(new Symbol(symbol));
        nos.set(new Price(px));
        nos.set(new OrderQty(qty));
        nos.set(new TimeInForce(TimeInForce.DAY));
        return nos;
    }

    public static OrderCancelReplaceRequest makeReplace(String origClOrdID, String symbol, char side, Double newPx, Double newQty) {
        // ctor: (ClOrdID, Side, TransactTime, OrdType)
        OrderCancelReplaceRequest rep = new OrderCancelReplaceRequest(
            new ClOrdID(nextClOrdID()),
            new Side(side),
            nowTT(),
            new OrdType(OrdType.LIMIT)
        );
        rep.set(new OrigClOrdID(origClOrdID));
        rep.set(new Symbol(symbol));
        if (newPx != null) rep.set(new Price(newPx));
        if (newQty != null) rep.set(new OrderQty(newQty));
        return rep;
    }
    
    public static boolean send(SessionID sid, Message msg) {
        try {
            return Session.sendToTarget(msg, sid);
        } catch (SessionNotFound e) {
            System.err.println("Session not found for " + sid + ": " + e.getMessage());
            return false;
        }
    }

    // normalize B/S to FIX 1/2 (add if you don't already have it)
    private static char normSide(char s) {
        s = Character.toUpperCase(s);
        if (s == 'B') return '1'; // BUY
        if (s == 'S') return '2'; // SELL
        return s;                 // assume already '1' or '2'
    }

    // NEW overload: same as 3-arg, but optionally sets OrderID(37)
    public static quickfix.fix50sp2.OrderCancelRequest makeCancel(
            String origClOrdID, String symbol, char side, String orderIDOpt) {

        quickfix.fix50sp2.OrderCancelRequest cxl =
            new quickfix.fix50sp2.OrderCancelRequest(
                new quickfix.field.ClOrdID(nextClOrdID()),
                new quickfix.field.Side(normSide(side)),
                nowTT()
            );

        cxl.set(new quickfix.field.OrigClOrdID(origClOrdID));
        cxl.set(new quickfix.field.Symbol(symbol));

        if (orderIDOpt != null && !orderIDOpt.isEmpty()) {
            cxl.set(new quickfix.field.OrderID(orderIDOpt)); // tag 37
        }
        return cxl;
    }    
}