package jmri.jmrix.openlcb.hub;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-peer counters. All atomic so updates from reader, writer, and broadcast
 * threads do not need synchronization.
 */
public final class PeerStats {

    private final long connectedAtMillis = System.currentTimeMillis();

    private final AtomicLong framesIn       = new AtomicLong();
    private final AtomicLong framesOut      = new AtomicLong();
    private final AtomicInteger queueDepthHighWater = new AtomicInteger();
    private final AtomicLong lastWriteLatencyNanos = new AtomicLong();
    private final AtomicLong totalBlockedNanos = new AtomicLong();

    public void recordInboundFrame()  { framesIn.incrementAndGet();  }
    public void recordOutboundFrame() { framesOut.incrementAndGet(); }

    public void recordQueueDepth(int depth) {
        int previous;
        do {
            previous = queueDepthHighWater.get();
            if (depth <= previous) return;
        } while (!queueDepthHighWater.compareAndSet(previous, depth));
    }

    public void recordWriteLatency(long nanos) { lastWriteLatencyNanos.set(nanos); }
    public void recordBlockedTime(long nanos)  { totalBlockedNanos.addAndGet(nanos); }

    public long framesIn()              { return framesIn.get(); }
    public long framesOut()             { return framesOut.get(); }
    public int  queueDepthHighWater()   { return queueDepthHighWater.get(); }
    public long lastWriteLatencyNanos() { return lastWriteLatencyNanos.get(); }
    public long totalBlockedNanos()     { return totalBlockedNanos.get(); }
    public long connectedAtMillis()     { return connectedAtMillis; }

    public String snapshot() {
        return String.format(
            "in=%d out=%d qHigh=%d lastWriteUs=%d blockedMs=%d",
            framesIn(), framesOut(), queueDepthHighWater(),
            lastWriteLatencyNanos() / 1000,
            totalBlockedNanos() / 1_000_000);
    }
}
