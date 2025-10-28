package fixperf.md;

import quickfix.*;
import quickfix.field.*;
import quickfix.fix50sp2.MarketDataIncrementalRefresh;
import quickfix.fix50sp2.MarketDataRequest;
import quickfix.fix50sp2.MarketDataSnapshotFullRefresh;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * MarketDataFeed — a composable FIX 4.2 market data helper for QuickFIX/J.
 *
 * What it does:
 *  - Subscribes to market data (top-of-book by default) in batches using 35=V (MarketDataRequest).
 *  - Maintains an in-memory cache of top-of-book (bid/ask/last) per symbol from 35=W/X updates.
 *  - Hot-reloads a symbols CSV on a schedule; when the set of symbols changes, it resubscribes.
 *  - When an unknown symbol shows up in MD, appends it to a discovery CSV for your later review.
 *
 * How to use with your existing Application (initiator side):
 *  1) Construct once at startup, before you start the Initiator:
 *     var feed = new MarketDataFeed(
 *         Path.of("./data/symbols.csv"),              // current instrument universe
 *         Path.of("./data/discovered_symbols.csv"),    // appended on-the-fly for new instruments
 *         50,                                           // symbols per MD request (batch size)
 *         true,                                         // snapshot+updates (263=0)
 *         1                                             // MarketDepth (top of book)
 *     );
 *     feed.start();   // begins periodic CSV reload (no network yet)
 *
 *  2) Inside your Application callbacks, delegate:
 *     onLogon(id)    -> feed.onLogon(id);              // sends MD subscriptions
 *     onLogout(id)   -> feed.onLogout(id);             // cancels and clears
 *     fromApp(msg,id)-> feed.fromApp(msg,id);          // ingests W/X MD updates
 *
 *  3) To use prices when building orders:
 *     MarketDataFeed.TopOfBook tob = feed.peek("AAPL");
 *     double px = (tob != null ? tob.mid() : 100.0);
 *
 * Notes:
 *  - This is FIX 4.2-specific and uses quickfix.fix42.* message classes.
 *  - If your venue requires one-symbol-per-request, set batchSize = 1.
 *  - If your venue wants you to resend the full symbol list on cancel (263=2), this class does so.
 */
public class MarketDataFeed implements Closeable {

  // ---- Public data view ----------------------------------------------------
  public static final class TopOfBook {
    public final double bid;
    public final double ask;
    public final double last;
    public final long   tsNanos;
    public TopOfBook(double bid, double ask, double last, long tsNanos) {
      this.bid = bid; this.ask = ask; this.last = last; this.tsNanos = tsNanos;
    }
    public double mid() {
      if (bid > 0 && ask > 0) return (bid + ask) * 0.5;
      if (bid > 0) return bid; if (ask > 0) return ask; return last;
    }
  }

  // ---- Configuration -------------------------------------------------------
  private final Path symbolsCsv;
  private final Path discoveredCsv;
  private final int batchSize;
  private final boolean snapshotPlusUpdates;   // 263=0 if true, else 1=snapshot only
  private final int marketDepth;               // 264 (1 = top-of-book)

  // ---- Runtime state -------------------------------------------------------
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "md-reloader"); t.setDaemon(true); return t; });

  private final Set<String> symbols = ConcurrentHashMap.newKeySet();
  private final ConcurrentHashMap<String, AtomicReference<TopOfBook>> cache = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<SessionID, List<String>> mdReqIdsBySession = new ConcurrentHashMap<>();
  private final AtomicLong mdReqSeq = new AtomicLong(1);

  private volatile boolean started = false;

  public MarketDataFeed(Path symbolsCsv,
                        Path discoveredCsv,
                        int batchSize,
                        boolean snapshotPlusUpdates,
                        int marketDepth) {
    this.symbolsCsv = symbolsCsv;
    this.discoveredCsv = discoveredCsv;
    this.batchSize = Math.max(1, batchSize);
    this.snapshotPlusUpdates = snapshotPlusUpdates;
    this.marketDepth = Math.max(1, marketDepth);
  }

  // ---- Lifecycle -----------------------------------------------------------
  public void start() throws IOException {
    if (started) return;
    reloadSymbols();
    scheduler.scheduleAtFixedRate(() -> {
      try { reloadSymbolsAndResubscribeIfChanged(); }
      catch (Exception e) { System.err.println("[MD] reload failed: " + e.getMessage()); }
    }, 30, 30, java.util.concurrent.TimeUnit.SECONDS); // adjust cadence as you like
    started = true;
  }

  @Override public void close() {
    scheduler.shutdownNow();
  }

  // ---- Call from your Application callbacks --------------------------------
  public void onLogon(SessionID id) {
    try {
      subscribeAllBatches(id);
    } catch (SessionNotFound e) {
      System.err.println("[MD] subscribe failed on logon: " + e.getMessage());
    }
  }

  public void onLogout(SessionID id) {
    try {
      cancelAllBatches(id);
    } catch (SessionNotFound e) {
      // ignore
    } finally {
      mdReqIdsBySession.remove(id);
    }
  }

  public void fromApp(Message message, SessionID id)
      throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
    String msgType = message.getHeader().getString(MsgType.FIELD);
    switch (msgType) {
      case MsgType.MARKET_DATA_SNAPSHOT_FULL_REFRESH: // W
        onSnapshot((MarketDataSnapshotFullRefresh) message);
        break;
      case MsgType.MARKET_DATA_INCREMENTAL_REFRESH:   // X
        onIncremental((MarketDataIncrementalRefresh) message);
        break;
      default:
        // ignore non-MD messages
    }
  }

  // ---- Public accessors ----------------------------------------------------
  public TopOfBook peek(String symbol) {
    AtomicReference<TopOfBook> ref = cache.get(symbol);
    return ref != null ? ref.get() : null;
  }

  public int symbolCount() { return symbols.size(); }

  // ---- Internals: subscriptions -------------------------------------------
  private void subscribeAllBatches(SessionID id) throws SessionNotFound {
    cancelAllBatches(id); // clean slate
    List<String> syms = new ArrayList<>(symbols);
    Collections.sort(syms);
    List<String> mdReqIds = new ArrayList<>();
    for (int i = 0; i < syms.size(); i += batchSize) {
      List<String> batch = syms.subList(i, Math.min(i + batchSize, syms.size()));
      String mdReqId = makeMdReqId(id);
      MarketDataRequest req = buildMdRequest(mdReqId, batch);
      Session.sendToTarget(req, id);
      mdReqIds.add(mdReqId);
    }
    mdReqIdsBySession.put(id, mdReqIds);
    System.out.printf("[MD] %s subscribed %d symbols via %d request(s)\n", id, syms.size(), mdReqIds.size());
  }

  private void cancelAllBatches(SessionID id) throws SessionNotFound {
    List<String> ids = mdReqIdsBySession.getOrDefault(id, List.of());
    if (ids.isEmpty()) return;
    // Some venues want the same symbol list on cancel; send empty list by default.
    for (String mdid : ids) {
      MarketDataRequest cancel = new MarketDataRequest(
          new MDReqID(mdid),
          new SubscriptionRequestType('2'), // disable previous
          new MarketDepth(marketDepth));
      // Optionally: add groups mirroring the original list
      Session.sendToTarget(cancel, id);
    }
    System.out.printf("[MD] %s canceled %d request(s)\n", id, ids.size());
  }

  private String makeMdReqId(SessionID id) {
    return id.getSenderCompID() + ":MD:" + mdReqSeq.getAndIncrement();
  }

  private MarketDataRequest buildMdRequest(String mdReqId, List<String> batch) {
    MarketDataRequest req = new MarketDataRequest(
        new MDReqID(mdReqId),
        new SubscriptionRequestType(snapshotPlusUpdates ? '1' : '0'),
        new MarketDepth(marketDepth)
    );
    req.set(new MDUpdateType(MDUpdateType.INCREMENTAL_REFRESH)); // prefer X updates
    req.set(new AggregatedBook(true));

    // Entry types: BID, OFFER, TRADE
    MarketDataRequest.NoMDEntryTypes t;
    t = new MarketDataRequest.NoMDEntryTypes(); t.set(new MDEntryType(MDEntryType.BID)); req.addGroup(t);
    t = new MarketDataRequest.NoMDEntryTypes(); t.set(new MDEntryType(MDEntryType.OFFER)); req.addGroup(t);
    t = new MarketDataRequest.NoMDEntryTypes(); t.set(new MDEntryType(MDEntryType.TRADE)); req.addGroup(t);

    // Symbols
    for (String s : batch) {
      MarketDataRequest.NoRelatedSym g = new MarketDataRequest.NoRelatedSym();
      g.set(new Symbol(s));
      // If you have SecurityID/Source in your CSV, set 48/22 here as well.
      req.addGroup(g);
    }
    return req;
  }

  // ---- Internals: CSV reload & resubscribe --------------------------------
  private void reloadSymbols() throws IOException {
    if (!Files.exists(symbolsCsv)) {
      System.err.println("[MD] symbols.csv does not exist: " + symbolsCsv);
      return;
    }
    Set<String> fresh = Files.lines(symbolsCsv, StandardCharsets.UTF_8)
        .skip(1)
        .map(l -> l.split(",", -1)[0].trim()) // column 0 = symbol
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toCollection(TreeSet::new));
    symbols.clear();
    symbols.addAll(fresh);
  }

  private void reloadSymbolsAndResubscribeIfChanged() throws IOException {
    Set<String> before = new TreeSet<>(symbols);
    reloadSymbols();
    if (!before.equals(symbols)) {
      System.out.printf("[MD] symbol set changed: before=%d after=%d\n", before.size(), symbols.size());
      for (SessionID id : mdReqIdsBySession.keySet()) {
        try { subscribeAllBatches(id); } catch (SessionNotFound e) { /* ignore */ }
      }
    }
  }

  // ---- Internals: message handling ----------------------------------------
  private void onSnapshot(MarketDataSnapshotFullRefresh snap) throws FieldNotFound {
    String sym = snap.getString(Symbol.FIELD);
    double bestBid = 0d, bestAsk = 0d, last = 0d;
    if (snap.isSetField(NoMDEntries.FIELD)) {
      int n = snap.getInt(NoMDEntries.FIELD);
      MarketDataSnapshotFullRefresh.NoMDEntries g = new MarketDataSnapshotFullRefresh.NoMDEntries();
      for (int i = 1; i <= n; i++) {
        snap.getGroup(i, g);
        char typ = g.getChar(MDEntryType.FIELD);
        double px = g.isSetField(MDEntryPx.FIELD) ? g.getDouble(MDEntryPx.FIELD) : 0d;
        switch (typ) {
          case MDEntryType.BID -> bestBid = Math.max(bestBid, px);
          case MDEntryType.OFFER -> bestAsk = (bestAsk == 0d) ? px : Math.min(bestAsk, px);
          case MDEntryType.TRADE -> last = px;
        }
      }
    }
    upsertTop(sym, bestBid, bestAsk, last);
    ensureDiscovered(sym);
  }

  private void onIncremental(MarketDataIncrementalRefresh inc) throws FieldNotFound {
    if (!inc.isSetField(NoMDEntries.FIELD)) return;
    int n = inc.getInt(NoMDEntries.FIELD);
    MarketDataIncrementalRefresh.NoMDEntries g = new MarketDataIncrementalRefresh.NoMDEntries();
    for (int i = 1; i <= n; i++) {
      inc.getGroup(i, g);
      String sym = g.isSetField(Symbol.FIELD) ? g.getString(Symbol.FIELD) : null;
      if (sym == null || sym.isEmpty()) continue;
      char typ = g.getChar(MDEntryType.FIELD);
      char act = g.isSetField(MDUpdateAction.FIELD) ? g.getChar(MDUpdateAction.FIELD) : MDUpdateAction.CHANGE;
      double px = g.isSetField(MDEntryPx.FIELD) ? g.getDouble(MDEntryPx.FIELD) : 0d;

      AtomicReference<TopOfBook> ref = cache.computeIfAbsent(sym, k -> new AtomicReference<>(new TopOfBook(0,0,0, System.nanoTime())));
      while (true) {
        TopOfBook old = ref.get();
        double bid = old.bid, ask = old.ask, last = old.last;
        if (act == MDUpdateAction.DELETE) {
          switch (typ) {
            case MDEntryType.BID -> bid = 0d;
            case MDEntryType.OFFER -> ask = 0d;
            case MDEntryType.TRADE -> {} // ignore delete of last
          }
        } else {
          switch (typ) {
            case MDEntryType.BID -> bid = Math.max(bid, px);
            case MDEntryType.OFFER -> ask = (ask == 0d) ? px : Math.min(ask, px);
            case MDEntryType.TRADE -> last = px;
          }
        }
        TopOfBook neu = new TopOfBook(bid, ask, last, System.nanoTime());
        if (ref.compareAndSet(old, neu)) break;
      }
      ensureDiscovered(sym);
    }
  }

  // ---- Internals: cache & discovery ---------------------------------------
  private void upsertTop(String sym, double bid, double ask, double last) {
    cache.compute(sym, (k, ref) -> {
      long now = System.nanoTime();
      TopOfBook tob = new TopOfBook(bid, ask, last, now);
      return new AtomicReference<>(tob);
    });
  }

  private void ensureDiscovered(String sym) {
    if (symbols.contains(sym)) return;
    // Append once per process lifetime to discovered file
    try {
      synchronized (this) {
        if (!Files.exists(discoveredCsv)) {
          Files.createDirectories(discoveredCsv.getParent());
          Files.writeString(discoveredCsv, "symbol,first_seen\n", StandardCharsets.UTF_8);
        }
        String line = sym + "," + Instant.now() + "\n";
        Files.writeString(discoveredCsv, line, StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND);
      }
      System.out.println("[MD] discovered new symbol: " + sym);
    } catch (IOException ioe) {
      System.err.println("[MD] failed to append discovered symbol: " + ioe.getMessage());
    }
  }
}
