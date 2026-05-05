package jmri.jmrix.openlcb.hub;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class HubTest {

    @Test
    void registerAndUnregisterMaintainsCount() {
        Hub h = new Hub();
        FakePeer a = new FakePeer("a", WireFormat.GRIDCONNECT);
        FakePeer b = new FakePeer("b", WireFormat.GRIDCONNECT);
        h.register(a);
        h.register(b);
        assertEquals(2, h.peerCount());
        h.unregister(a);
        assertEquals(1, h.peerCount());
    }

    @Test
    void broadcastSkipsSource() {
        Hub h = new Hub();
        FakePeer a = new FakePeer("a", WireFormat.GRIDCONNECT);
        FakePeer b = new FakePeer("b", WireFormat.GRIDCONNECT);
        FakePeer c = new FakePeer("c", WireFormat.GRIDCONNECT);
        h.register(a); h.register(b); h.register(c);

        h.broadcast(HubFrame.gridConnect("frame-1"), a);

        assertEquals(0, a.received.size(), "source must not receive its own frame");
        assertEquals(1, b.received.size());
        assertEquals(1, c.received.size());
    }

    @Test
    void broadcastSkipsDifferentWireFormat() {
        Hub h = new Hub();
        FakePeer gc1 = new FakePeer("gc1", WireFormat.GRIDCONNECT);
        FakePeer gc2 = new FakePeer("gc2", WireFormat.GRIDCONNECT);
        FakePeer tcp = new FakePeer("olcbtcp", WireFormat.OPENLCB_TCP);
        h.register(gc1); h.register(gc2); h.register(tcp);

        h.broadcast(HubFrame.gridConnect("gc-frame"), gc1);

        assertEquals(1, gc2.received.size());
        assertEquals(0, tcp.received.size(), "OPENLCB_TCP peer must not see GRIDCONNECT frames");
    }

    @Test
    void closeAllClosesEveryPeer() {
        Hub h = new Hub();
        List<FakePeer> peers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            FakePeer p = new FakePeer("p" + i, WireFormat.GRIDCONNECT);
            peers.add(p);
            h.register(p);
        }
        h.closeAll("test");
        for (FakePeer p : peers) assertFalse(p.open, "peer " + p.remoteAddress() + " should be closed");
    }

    @Test
    void broadcastSurvivesPeerThrowing() {
        Hub h = new Hub();
        FakePeer good = new FakePeer("good", WireFormat.GRIDCONNECT);
        FakePeer bad  = new FakePeer("bad",  WireFormat.GRIDCONNECT) {
            @Override public void send(HubFrame f) { throw new RuntimeException("boom"); }
        };
        FakePeer good2 = new FakePeer("good2", WireFormat.GRIDCONNECT);
        FakePeer source = new FakePeer("src", WireFormat.GRIDCONNECT);
        h.register(source); h.register(good); h.register(bad); h.register(good2);

        h.broadcast(HubFrame.gridConnect("x"), source);

        assertEquals(1, good.received.size());
        assertEquals(1, good2.received.size());
        assertFalse(bad.open, "bad peer should have been closed by the hub");
    }

    @Test
    void frameListenerNotifiedBeforeFanOut() {
        Hub h = new Hub();
        FakePeer a = new FakePeer("a", WireFormat.GRIDCONNECT);
        FakePeer b = new FakePeer("b", WireFormat.GRIDCONNECT);
        h.register(a); h.register(b);

        List<HubFrame> seen = new ArrayList<>();
        h.addFrameListener((frame, src) -> seen.add(frame));

        HubFrame f = HubFrame.gridConnect("hi");
        h.broadcast(f, a);

        assertEquals(1, seen.size());
        assertSame(f, seen.get(0));
    }

    /** Minimal HubPeer fixture; not async — receives synchronously into a list. */
    static class FakePeer implements HubPeer {
        final String name;
        final WireFormat fmt;
        final List<HubFrame> received = new ArrayList<>();
        final PeerStats stats = new PeerStats();
        volatile boolean open = true;
        FakePeer(String name, WireFormat fmt) { this.name = name; this.fmt = fmt; }
        @Override public String remoteAddress() { return name; }
        @Override public WireFormat wireFormat() { return fmt; }
        @Override public void send(HubFrame frame) { received.add(frame); }
        @Override public void close(String reason) { open = false; }
        @Override public boolean isOpen() { return open; }
        @Override public PeerStats stats() { return stats; }
    }
}
