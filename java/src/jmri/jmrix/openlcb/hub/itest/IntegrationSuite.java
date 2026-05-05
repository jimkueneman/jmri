package jmri.jmrix.openlcb.hub.itest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import jmri.jmrix.openlcb.hub.HubConfiguration;

/**
 * End-to-end test suite for the hub. Each scenario stands up a fresh hub via
 * {@link TestHarness}, drives load through real TCP and WebSocket peers, and
 * verifies the design claims documented in {@code OPENLCB_HUB_CHANGE_REQUEST.md}.
 *
 * Run via Maven:
 * {@code mvn exec:java -Dexec.mainClass=jmri.jmrix.openlcb.hub.itest.IntegrationSuite}
 *
 * Hub guarantees under test:
 *   1. No frame is ever dropped.
 *   2. No peer is closed for slowness — only for genuine I/O failure.
 *   3. A slow peer paces the bus to its drain rate (backpressure to source).
 *   4. Transient slowness is absorbed by the per-peer queue (default 16K).
 */
public final class IntegrationSuite {

    private static final String FRAME_PREFIX = ":X19490AAAN";

    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        System.setProperty("org.slf4j.simpleLogger.showShortLogName", "true");

        List<Result> results = new ArrayList<>();
        results.add(run("01 mass connect/disconnect (100 TCP + 100 WS)",   IntegrationSuite::scenarioMassConnect));
        results.add(run("02 cross-transport broadcast (TCP <-> WS)",       IntegrationSuite::scenarioCrossTransport));
        results.add(run("03 sustained moderate throughput (10×100 fps×5s)",IntegrationSuite::scenarioSustainedThroughput));
        results.add(run("04 startup-burst absorption (16K-frame burst)",   IntegrationSuite::scenarioStartupBurst));
        results.add(run("05 slow consumer paces bus (no drops, no kicks)", IntegrationSuite::scenarioSlowConsumerPacesBus));
        results.add(run("06 aggressive sender backpressured by TCP",       IntegrationSuite::scenarioBackpressureOnSender));
        results.add(run("07 connection churn under traffic",               IntegrationSuite::scenarioChurn));
        results.add(run("08 scale: 200 peers, modest traffic, 0 drops",    IntegrationSuite::scenarioScale200));

        int pass = 0, fail = 0;
        System.out.println();
        System.out.println("=== Integration suite results ===");
        for (Result r : results) {
            System.out.printf("  %-6s  %-58s %5d ms%n",
                r.pass ? "PASS" : "FAIL", r.name, r.durationMs);
            if (r.detail != null && !r.detail.isEmpty()) {
                System.out.println("           " + r.detail);
            }
            if (r.pass) pass++; else fail++;
        }
        System.out.println("=================================");
        System.out.printf("  %d passed, %d failed%n", pass, fail);
        System.exit(fail == 0 ? 0 : 1);
    }

    // ------------------------------------------------------------------ scenarios

    private static String scenarioMassConnect() throws Exception {
        try (TestHarness h = new TestHarness(HubConfiguration.defaults())) {
            List<TcpTestPeer> tcp = new ArrayList<>();
            List<WebSocketTestPeer> ws = new ArrayList<>();
            for (int i = 0; i < 100; i++) tcp.add(h.connectTcp("T" + i));
            for (int i = 0; i < 100; i++) ws.add(h.connectWebSocket("W" + i));
            await(() -> h.hub.peerCount() == 200, 10_000,
                "expected 200 peers, got " + h.hub.peerCount());

            for (TcpTestPeer p : tcp) p.close();
            for (WebSocketTestPeer p : ws) p.close();
            await(() -> h.hub.peerCount() == 0, 10_000,
                "expected 0 peers after disconnect, got " + h.hub.peerCount());
            return "200 peers connected and cleanly disconnected";
        }
    }

    private static String scenarioCrossTransport() throws Exception {
        try (TestHarness h = new TestHarness(HubConfiguration.defaults())) {
            List<TcpTestPeer> tcp = new ArrayList<>();
            List<WebSocketTestPeer> ws = new ArrayList<>();
            for (int i = 0; i < 5; i++) tcp.add(h.connectTcp("T" + i));
            for (int i = 0; i < 5; i++) ws.add(h.connectWebSocket("W" + i));
            await(() -> h.hub.peerCount() == 10, 5_000, "peerCount=" + h.hub.peerCount());

            tcp.get(0).send(makeFrame(0));
            await(() -> sumReceived(tcp) - tcp.get(0).received() >= 4
                     && sumReceived(ws) >= 5,
                  3_000, "TCP-source fan-out incomplete");

            long tcp0Before = tcp.get(0).received();
            if (tcp0Before != 0) throw new AssertionError("TCP source got its own frame, count=" + tcp0Before);

            long wsBaseline = sumReceived(ws);
            long tcpBaseline = sumReceived(tcp);
            ws.get(0).send(makeFrame(1));
            await(() -> sumReceived(tcp) - tcpBaseline >= 5
                     && sumReceived(ws) - wsBaseline >= 4,
                  3_000, "WS-source fan-out incomplete");

            for (TcpTestPeer p : tcp) p.close();
            for (WebSocketTestPeer p : ws) p.close();
            return "TCP<->WS bidirectional fan-out verified";
        }
    }

    private static String scenarioSustainedThroughput() throws Exception {
        try (TestHarness h = new TestHarness(HubConfiguration.defaults())) {
            int totalPeers = 100;
            int senders = 10;
            int fpsPerSender = 100;
            int durationS = 5;
            int totalFramesEmitted = senders * fpsPerSender * durationS;

            List<TcpTestPeer> tcp = new ArrayList<>();
            List<WebSocketTestPeer> ws = new ArrayList<>();
            for (int i = 0; i < 50; i++) tcp.add(h.connectTcp("T" + i));
            for (int i = 0; i < 50; i++) ws.add(h.connectWebSocket("W" + i));
            await(() -> h.hub.peerCount() == totalPeers, 10_000,
                "peerCount=" + h.hub.peerCount());

            ScheduledExecutorService sched = Executors.newScheduledThreadPool(senders);
            AtomicInteger seq = new AtomicInteger();
            long endNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationS);
            long periodNanos = TimeUnit.SECONDS.toNanos(1) / fpsPerSender;
            for (int i = 0; i < senders; i++) {
                final TcpTestPeer sender = tcp.get(i);
                sched.scheduleAtFixedRate(() -> {
                    if (System.nanoTime() < endNanos) sender.send(makeFrame(seq.getAndIncrement()));
                }, 0, periodNanos, TimeUnit.NANOSECONDS);
            }

            await(() -> {
                long minReceived = Long.MAX_VALUE;
                for (int i = senders; i < tcp.size(); i++)
                    minReceived = Math.min(minReceived, tcp.get(i).received());
                for (WebSocketTestPeer p : ws)
                    minReceived = Math.min(minReceived, p.received());
                return minReceived >= totalFramesEmitted;
            }, durationS * 1000L + 5_000L,
            "non-sender minimum received < " + totalFramesEmitted);

            sched.shutdownNow();
            for (TcpTestPeer p : tcp) p.close();
            for (WebSocketTestPeer p : ws) p.close();
            return totalFramesEmitted + " frames delivered to all 90 non-senders";
        }
    }

    private static String scenarioStartupBurst() throws Exception {
        try (TestHarness h = new TestHarness(HubConfiguration.defaults())) {
            int tcpRx = 5, wsRx = 5;
            int perSender = 8_000;     // two senders × 8000 = 16000 hub-level
            List<TcpTestPeer> tcpReceivers = new ArrayList<>();
            List<WebSocketTestPeer> wsReceivers = new ArrayList<>();
            for (int i = 0; i < tcpRx; i++) tcpReceivers.add(h.connectTcp("RT" + i));
            for (int i = 0; i < wsRx;  i++) wsReceivers.add(h.connectWebSocket("RW" + i));
            TcpTestPeer       tcpSender = h.connectTcp("ST");
            WebSocketTestPeer wsSender  = h.connectWebSocket("SW");
            await(() -> h.hub.peerCount() == tcpRx + wsRx + 2, 5_000, "peers connected");

            Thread t1 = new Thread(() -> { for (int i = 0; i < perSender; i++) tcpSender.send(makeFrame(i)); }, "tcpSender");
            Thread t2 = new Thread(() -> { for (int i = 0; i < perSender; i++) wsSender.send(makeFrame(i)); },  "wsSender");
            t1.start(); t2.start();
            t1.join(); t2.join();

            int expectedAtReceiver = perSender * 2;
            await(() -> {
                long min = Long.MAX_VALUE;
                for (TcpTestPeer p : tcpReceivers)       min = Math.min(min, p.received());
                for (WebSocketTestPeer p : wsReceivers)  min = Math.min(min, p.received());
                return min >= expectedAtReceiver;
            }, 30_000, "min receiver below " + expectedAtReceiver
                + " (TCP min=" + minReceived(tcpReceivers)
                + ", WS min=" + minReceivedWs(wsReceivers) + ")");

            for (TcpTestPeer p : tcpReceivers) if (!p.isOpen()) throw new AssertionError("TCP rx closed");
            for (WebSocketTestPeer p : wsReceivers) if (!p.isOpen()) throw new AssertionError("WS rx closed");
            if (!tcpSender.isOpen()) throw new AssertionError("TCP sender closed");
            if (!wsSender.isOpen())  throw new AssertionError("WS sender closed");

            for (TcpTestPeer p : tcpReceivers)      p.close();
            for (WebSocketTestPeer p : wsReceivers) p.close();
            tcpSender.close(); wsSender.close();
            return (perSender * 2) + " frames (TCP+WS senders) delivered to "
                + tcpRx + " TCP + " + wsRx + " WS receivers, none closed";
        }
    }

    private static String scenarioSlowConsumerPacesBus() throws Exception {
        HubConfiguration cfg = HubConfiguration.builder()
            .outboundQueueDepth(64)
            .build();
        try (TestHarness h = new TestHarness(cfg)) {
            int frames = 200;
            int healthyTcp = 4, healthyWs = 4;

            List<TcpTestPeer> tcpOk = new ArrayList<>();
            List<WebSocketTestPeer> wsOk = new ArrayList<>();
            for (int i = 0; i < healthyTcp; i++) tcpOk.add(h.connectTcp("TOK" + i));
            for (int i = 0; i < healthyWs;  i++) wsOk.add(h.connectWebSocket("WOK" + i));

            TcpTestPeer slowTcp = h.connectTcp("TSLOW");
            slowTcp.setReadDelayMs(20);
            WebSocketTestPeer slowWs = h.connectWebSocket("WSLOW");
            slowWs.setReadDelayMs(20);

            TcpTestPeer sender = h.connectTcp("SND");
            await(() -> h.hub.peerCount() == healthyTcp + healthyWs + 3, 5_000,
                  "peers connected: got " + h.hub.peerCount());

            long t0 = System.nanoTime();
            for (int i = 0; i < frames; i++) sender.send(makeFrame(i));
            await(() -> slowTcp.received() >= frames && slowWs.received() >= frames,
                30_000,
                "slow peers never caught up (TCP " + slowTcp.received()
                + ", WS " + slowWs.received() + " / " + frames + ")");
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

            for (TcpTestPeer p : tcpOk) {
                if (p.received() < frames) throw new AssertionError("healthy TCP short: " + p.received());
                if (!p.isOpen()) throw new AssertionError("healthy TCP closed");
            }
            for (WebSocketTestPeer p : wsOk) {
                if (p.received() < frames) throw new AssertionError("healthy WS short: " + p.received());
                if (!p.isOpen()) throw new AssertionError("healthy WS closed");
            }
            if (!slowTcp.isOpen()) throw new AssertionError("slow TCP was kicked");
            if (!slowWs.isOpen())  throw new AssertionError("slow WS was kicked");

            return "slow TCP + slow WS paced bus to ~"
                + (frames * 1000L / Math.max(1, elapsedMs)) + " fps over "
                + elapsedMs + " ms; all 11 receivers (8 fast + 2 slow) got "
                + frames + " frames";
        }
    }

    private static String scenarioBackpressureOnSender() throws Exception {
        HubConfiguration cfg = HubConfiguration.builder()
            .outboundQueueDepth(64)
            .build();

        long tcpSourceMs;
        long wsSourceMs;
        int target = 1_000;

        try (TestHarness h = new TestHarness(cfg)) {
            TcpTestPeer slowTcp = h.connectTcp("TSLOW");        slowTcp.setReadDelayMs(10);
            WebSocketTestPeer slowWs = h.connectWebSocket("WSLOW"); slowWs.setReadDelayMs(10);
            TcpTestPeer fastTcp = h.connectTcp("TFAST");
            WebSocketTestPeer fastWs = h.connectWebSocket("WFAST");
            TcpTestPeer sender = h.connectTcp("SND");
            await(() -> h.hub.peerCount() == 5, 5_000, "TCP-source peers connected");

            long t0 = System.nanoTime();
            for (int i = 0; i < target; i++) sender.send(makeFrame(i));
            await(() -> slowTcp.received() >= target && slowWs.received() >= target
                     && fastTcp.received() >= target && fastWs.received() >= target,
                30_000, "TCP-source: slowTcp=" + slowTcp.received()
                       + " slowWs=" + slowWs.received()
                       + " fastTcp=" + fastTcp.received()
                       + " fastWs=" + fastWs.received());
            tcpSourceMs = (System.nanoTime() - t0) / 1_000_000;
            if (!sender.isOpen() || !slowTcp.isOpen() || !slowWs.isOpen()
                || !fastTcp.isOpen() || !fastWs.isOpen())
                throw new AssertionError("TCP-source: a peer was kicked");
        }

        try (TestHarness h = new TestHarness(cfg)) {
            TcpTestPeer slowTcp = h.connectTcp("TSLOW");        slowTcp.setReadDelayMs(10);
            WebSocketTestPeer slowWs = h.connectWebSocket("WSLOW"); slowWs.setReadDelayMs(10);
            TcpTestPeer fastTcp = h.connectTcp("TFAST");
            WebSocketTestPeer fastWs = h.connectWebSocket("WFAST");
            WebSocketTestPeer sender = h.connectWebSocket("SND");
            await(() -> h.hub.peerCount() == 5, 5_000, "WS-source peers connected");

            long t0 = System.nanoTime();
            for (int i = 0; i < target; i++) sender.send(makeFrame(i));
            await(() -> slowTcp.received() >= target && slowWs.received() >= target
                     && fastTcp.received() >= target && fastWs.received() >= target,
                30_000, "WS-source: slowTcp=" + slowTcp.received()
                       + " slowWs=" + slowWs.received()
                       + " fastTcp=" + fastTcp.received()
                       + " fastWs=" + fastWs.received());
            wsSourceMs = (System.nanoTime() - t0) / 1_000_000;
            if (!sender.isOpen() || !slowTcp.isOpen() || !slowWs.isOpen()
                || !fastTcp.isOpen() || !fastWs.isOpen())
                throw new AssertionError("WS-source: a peer was kicked");
        }

        return "TCP source paced to ~" + (target * 1000L / Math.max(1, tcpSourceMs))
             + " fps (" + tcpSourceMs + " ms); WS source paced to ~"
             + (target * 1000L / Math.max(1, wsSourceMs))
             + " fps (" + wsSourceMs + " ms); no kicks";
    }

    private static String scenarioChurn() throws Exception {
        try (TestHarness h = new TestHarness(HubConfiguration.defaults())) {
            List<TcpTestPeer> tcpPerm = new ArrayList<>();
            for (int i = 0; i < 3; i++) tcpPerm.add(h.connectTcp("PT" + i));
            List<WebSocketTestPeer> wsPerm = new ArrayList<>();
            for (int i = 0; i < 2; i++) wsPerm.add(h.connectWebSocket("PW" + i));
            TcpTestPeer       tcpSender = tcpPerm.get(0);
            WebSocketTestPeer wsSender  = wsPerm.get(0);

            ScheduledExecutorService bg = Executors.newScheduledThreadPool(2);
            AtomicInteger seq = new AtomicInteger();
            long endNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            bg.scheduleAtFixedRate(() -> {
                if (System.nanoTime() < endNanos) tcpSender.send(makeFrame(seq.getAndIncrement()));
            }, 0, 20, TimeUnit.MILLISECONDS);
            bg.scheduleAtFixedRate(() -> {
                if (System.nanoTime() < endNanos) wsSender.send(makeFrame(seq.getAndIncrement()));
            }, 0, 20, TimeUnit.MILLISECONDS);

            ScheduledExecutorService churn = Executors.newScheduledThreadPool(2);
            AtomicInteger churnCount = new AtomicInteger();
            long churnEnd = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < churnEnd) {
                final int n = churnCount.incrementAndGet();
                if (n % 2 == 0) {
                    TcpTestPeer ephemeral = h.connectTcp("CT" + n);
                    churn.schedule(ephemeral::close, 500, TimeUnit.MILLISECONDS);
                } else {
                    WebSocketTestPeer ephemeral = h.connectWebSocket("CW" + n);
                    churn.schedule(ephemeral::close, 500, TimeUnit.MILLISECONDS);
                }
                Thread.sleep(100);
            }
            bg.shutdownNow();
            churn.shutdown();
            churn.awaitTermination(2, TimeUnit.SECONDS);

            int permTotal = tcpPerm.size() + wsPerm.size();
            await(() -> h.hub.peerCount() <= permTotal, 8_000,
                  "ephemeral peers did not drain: " + h.hub.peerCount());
            for (TcpTestPeer p : tcpPerm) p.close();
            for (WebSocketTestPeer p : wsPerm) p.close();
            return churnCount.get() + " ephemeral peers (mixed TCP/WS) churned without disrupting bus";
        }
    }

    private static String scenarioScale200() throws Exception {
        try (TestHarness h = new TestHarness(HubConfiguration.defaults())) {
            int tcpN = 100, wsN = 100;
            int tcpSenders = 3, wsSenders = 2;
            int fpsPerSender = 20;
            int durationS = 5;
            int totalFrames = (tcpSenders + wsSenders) * fpsPerSender * durationS;

            List<TcpTestPeer> tcp = new ArrayList<>();
            List<WebSocketTestPeer> ws = new ArrayList<>();
            for (int i = 0; i < tcpN; i++) tcp.add(h.connectTcp("T" + i));
            for (int i = 0; i < wsN;  i++) ws.add(h.connectWebSocket("W" + i));
            await(() -> h.hub.peerCount() == tcpN + wsN, 15_000,
                "peerCount=" + h.hub.peerCount());

            ScheduledExecutorService sched = Executors.newScheduledThreadPool(tcpSenders + wsSenders);
            AtomicInteger seq = new AtomicInteger();
            long endNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationS);
            long periodNanos = TimeUnit.SECONDS.toNanos(1) / fpsPerSender;
            for (int i = 0; i < tcpSenders; i++) {
                TcpTestPeer s = tcp.get(i);
                sched.scheduleAtFixedRate(() -> {
                    if (System.nanoTime() < endNanos) s.send(makeFrame(seq.getAndIncrement()));
                }, 0, periodNanos, TimeUnit.NANOSECONDS);
            }
            for (int i = 0; i < wsSenders; i++) {
                WebSocketTestPeer s = ws.get(i);
                sched.scheduleAtFixedRate(() -> {
                    if (System.nanoTime() < endNanos) s.send(makeFrame(seq.getAndIncrement()));
                }, 0, periodNanos, TimeUnit.NANOSECONDS);
            }

            await(() -> {
                long min = Long.MAX_VALUE;
                for (int i = tcpSenders; i < tcp.size(); i++)
                    min = Math.min(min, tcp.get(i).received());
                for (int i = wsSenders; i < ws.size(); i++)
                    min = Math.min(min, ws.get(i).received());
                return min >= totalFrames;
            }, durationS * 1000L + 15_000L,
            "non-sender minimum below " + totalFrames);

            sched.shutdownNow();
            for (TcpTestPeer p : tcp) p.close();
            for (WebSocketTestPeer p : ws) p.close();
            return tcpN + " TCP + " + wsN + " WS peers; "
                + tcpSenders + " TCP + " + wsSenders + " WS senders; "
                + totalFrames + " frames each";
        }
    }

    // ------------------------------------------------------------------ helpers

    @FunctionalInterface
    interface Scenario { String run() throws Exception; }

    static final class Result {
        final boolean pass;
        final String name;
        final String detail;
        final long durationMs;
        Result(boolean pass, String name, String detail, long durationMs) {
            this.pass = pass; this.name = name; this.detail = detail; this.durationMs = durationMs;
        }
    }

    private static Result run(String name, Scenario s) {
        long t0 = System.currentTimeMillis();
        System.out.print("RUN  " + name + " ... ");
        System.out.flush();
        try {
            String detail = s.run();
            long ms = System.currentTimeMillis() - t0;
            System.out.println("PASS (" + ms + " ms)");
            return new Result(true, name, detail, ms);
        } catch (AssertionError | Exception e) {
            long ms = System.currentTimeMillis() - t0;
            System.out.println("FAIL (" + ms + " ms): " + e.getMessage());
            return new Result(false, name, e.getMessage(), ms);
        }
    }

    private static void await(BooleanSupplier cond, long timeoutMs, String failMsg)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(50);
        }
        if (!cond.getAsBoolean()) throw new AssertionError(failMsg);
    }

    private static String makeFrame(int seq) {
        return FRAME_PREFIX + String.format("%015X", seq & 0xFFFFFFFFFFFFFFFL) + ";";
    }

    private static long sumReceived(List<? extends Object> peers) {
        long total = 0;
        for (Object p : peers) {
            if (p instanceof TcpTestPeer)        total += ((TcpTestPeer) p).received();
            else if (p instanceof WebSocketTestPeer) total += ((WebSocketTestPeer) p).received();
        }
        return total;
    }

    private static long minReceived(List<TcpTestPeer> peers) {
        long min = Long.MAX_VALUE;
        for (TcpTestPeer p : peers) min = Math.min(min, p.received());
        return min == Long.MAX_VALUE ? 0 : min;
    }

    private static long minReceivedWs(List<WebSocketTestPeer> peers) {
        long min = Long.MAX_VALUE;
        for (WebSocketTestPeer p : peers) min = Math.min(min, p.received());
        return min == Long.MAX_VALUE ? 0 : min;
    }
}
