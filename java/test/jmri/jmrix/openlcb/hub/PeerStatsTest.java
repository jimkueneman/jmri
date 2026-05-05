package jmri.jmrix.openlcb.hub;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PeerStatsTest {

    @Test
    void countersStartAtZero() {
        PeerStats s = new PeerStats();
        assertEquals(0, s.framesIn());
        assertEquals(0, s.framesOut());
        assertEquals(0, s.queueDepthHighWater());
        assertEquals(0, s.totalBlockedNanos());
    }

    @Test
    void recordsAreCumulative() {
        PeerStats s = new PeerStats();
        for (int i = 0; i < 100; i++) s.recordInboundFrame();
        for (int i = 0; i < 50; i++) s.recordOutboundFrame();
        assertEquals(100, s.framesIn());
        assertEquals(50, s.framesOut());
    }

    @Test
    void queueDepthHighWaterMonotonic() {
        PeerStats s = new PeerStats();
        s.recordQueueDepth(5);
        s.recordQueueDepth(20);
        s.recordQueueDepth(10);   // should not lower the high-water
        s.recordQueueDepth(50);
        s.recordQueueDepth(30);
        assertEquals(50, s.queueDepthHighWater());
    }

    @Test
    void snapshotIsHumanReadable() {
        PeerStats s = new PeerStats();
        s.recordInboundFrame();
        String snap = s.snapshot();
        assertTrue(snap.contains("in=1"));
        assertTrue(snap.contains("out=0"));
    }
}
