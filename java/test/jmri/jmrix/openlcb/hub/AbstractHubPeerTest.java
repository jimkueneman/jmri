package jmri.jmrix.openlcb.hub;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class AbstractHubPeerTest {

    @Test
    void closeIsIdempotent() {
        Hub h = new Hub();
        AtomicInteger closeCalls = new AtomicInteger();
        TestPeer p = new TestPeer(h, HubConfiguration.defaults(), () -> {}, closeCalls);
        h.register(p);

        p.close("first");
        p.close("second");
        p.close("third");

        assertEquals(1, closeCalls.get());
        assertFalse(p.isOpen());
    }

    @Test
    void sendBlocksWhenQueueFullAndUnblocksOnDrain() throws Exception {
        Hub h = new Hub();
        HubConfiguration cfg = HubConfiguration.builder()
            .outboundQueueDepth(4)
            .build();
        TestPeer p = new TestPeer(h, cfg, () -> {}, new AtomicInteger());
        h.register(p);

        // Fill the queue to capacity
        for (int i = 0; i < 4; i++) p.send(HubFrame.gridConnect("f" + i));
        assertEquals(4, p.outboundDepthForTest());

        // The next send must BLOCK, not drop. Spawn a thread to attempt it.
        CountDownLatch blocked = new CountDownLatch(1);
        AtomicBoolean returned = new AtomicBoolean();
        Thread t = new Thread(() -> {
            blocked.countDown();
            p.send(HubFrame.gridConnect("blocked"));
            returned.set(true);
        });
        t.start();
        blocked.await();
        Thread.sleep(80);
        assertFalse(returned.get(), "send must block while queue is full");

        // Drain one slot — the blocked send should unblock and complete
        p.pollOutboundForTest();
        t.join(2_000);
        assertTrue(returned.get(), "send must unblock after queue drains");
        assertTrue(p.isOpen(), "peer must remain open under backpressure");
    }

    @Test
    void sendOnClosedPeerReturnsImmediately() throws Exception {
        Hub h = new Hub();
        HubConfiguration cfg = HubConfiguration.builder().outboundQueueDepth(2).build();
        TestPeer p = new TestPeer(h, cfg, () -> {}, new AtomicInteger());
        h.register(p);

        p.send(HubFrame.gridConnect("a"));
        p.send(HubFrame.gridConnect("b"));
        p.close("test");

        long t0 = System.nanoTime();
        p.send(HubFrame.gridConnect("c"));      // must NOT block
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(elapsedMs < 100, "send to closed peer should return promptly, took " + elapsedMs + " ms");
    }

    @Test
    void closeUnblocksWaitingSender() throws Exception {
        Hub h = new Hub();
        HubConfiguration cfg = HubConfiguration.builder().outboundQueueDepth(1).build();
        TestPeer p = new TestPeer(h, cfg, () -> {}, new AtomicInteger());
        h.register(p);

        p.send(HubFrame.gridConnect("a"));
        AtomicBoolean done = new AtomicBoolean();
        Thread t = new Thread(() -> {
            p.send(HubFrame.gridConnect("blocked"));
            done.set(true);
        });
        t.start();
        Thread.sleep(80);
        assertFalse(done.get(), "send should be blocked");

        p.close("forced");
        t.join(2_000);
        assertTrue(done.get(), "close must unblock waiting sender");
    }

    @Test
    void inboundFrameTooLargeClosesPeer() {
        Hub h = new Hub();
        HubConfiguration cfg = HubConfiguration.builder()
            .maxFramePayloadBytes(10)
            .build();
        TestPeer p = new TestPeer(h, cfg, () -> {}, new AtomicInteger());
        h.register(p);

        boolean accepted = p.callAccept("THIS_LINE_IS_TOO_LONG_FOR_THE_LIMIT");
        assertFalse(accepted);
        assertFalse(p.isOpen());
    }

    @Test
    void onFrameQueuedFiresOncePerSend() {
        Hub h = new Hub();
        AtomicInteger queuedCalls = new AtomicInteger();
        HubConfiguration cfg = HubConfiguration.builder().outboundQueueDepth(100).build();
        TestPeer p = new TestPeer(h, cfg, queuedCalls::incrementAndGet, new AtomicInteger());
        h.register(p);

        for (int i = 0; i < 50; i++) p.send(HubFrame.gridConnect("f" + i));
        assertEquals(50, queuedCalls.get());
    }

    /** Minimal concrete subclass exposing the protected hooks for tests. */
    static class TestPeer extends AbstractHubPeer {
        final Runnable onQueued;
        final AtomicInteger closeCalls;
        TestPeer(Hub h, HubConfiguration cfg, Runnable onQueued, AtomicInteger closeCalls) {
            super(h, cfg, "test", WireFormat.GRIDCONNECT);
            this.onQueued = onQueued;
            this.closeCalls = closeCalls;
        }
        @Override protected void onFrameQueued() { onQueued.run(); }
        @Override protected void doClose(String reason) { closeCalls.incrementAndGet(); }
        boolean callAccept(String line) { return acceptInbound(line); }
        int    outboundDepthForTest() { return outboundDepth(); }
        HubFrame pollOutboundForTest() { return pollOutbound(); }
    }
}
