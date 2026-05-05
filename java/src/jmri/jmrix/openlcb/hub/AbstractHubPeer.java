package jmri.jmrix.openlcb.hub;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared infrastructure for every {@link HubPeer} implementation: bounded
 * outbound queue with blocking-put backpressure, stats, idempotent close.
 *
 * Subclasses provide the transport-specific drain mechanism via
 * {@link #onFrameQueued()} and the close action via {@link #doClose(String)}.
 *
 * Reliability premise: OpenLCB requires every frame to be delivered to every
 * connected peer. {@link #send} therefore <em>blocks</em> when the outbound
 * queue is full, propagating backpressure through the broadcasting thread
 * back to the original source's TCP socket. There is no drop path and no
 * rate limit. A peer is removed only on I/O failure or explicit close.
 */
public abstract class AbstractHubPeer implements HubPeer {

    private static final Logger log = LoggerFactory.getLogger(AbstractHubPeer.class);

    private final Hub hub;
    private final HubConfiguration config;
    private final String remoteAddress;
    private final WireFormat wireFormat;

    private final ArrayBlockingQueue<HubFrame> outbound;
    private final PeerStats stats = new PeerStats();
    private final AtomicBoolean open = new AtomicBoolean(true);

    protected AbstractHubPeer(Hub hub,
                              HubConfiguration config,
                              String remoteAddress,
                              WireFormat wireFormat) {
        this.hub = hub;
        this.config = config;
        this.remoteAddress = remoteAddress;
        this.wireFormat = wireFormat;
        this.outbound = new ArrayBlockingQueue<>(config.outboundQueueDepth);
    }

    @Override public final String remoteAddress() { return remoteAddress; }
    @Override public final WireFormat wireFormat() { return wireFormat; }
    @Override public final boolean isOpen()        { return open.get(); }
    @Override public final PeerStats stats()       { return stats; }

    /**
     * Hub-to-peer delivery. Blocks the calling (broadcasting) thread when
     * the outbound queue is full — this is the backpressure path that paces
     * the original source. If the peer is already closed, returns silently.
     * If the calling thread is interrupted while waiting, the interrupt flag
     * is preserved and the call returns without delivering.
     */
    @Override
    public final void send(HubFrame frame) {
        if (!open.get()) return;
        long t0 = System.nanoTime();
        try {
            outbound.put(frame);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        long blockedNanos = System.nanoTime() - t0;
        if (blockedNanos > 1_000_000) stats.recordBlockedTime(blockedNanos);  // > 1 ms
        stats.recordQueueDepth(outbound.size());
        onFrameQueued();
    }

    /**
     * Called once per successful enqueue. Subclass wakes its drain mechanism
     * (start a writeAsync, signal a writer thread, etc.). May be called from
     * any thread; must be cheap and non-blocking.
     */
    protected abstract void onFrameQueued();

    /** Subclass close: stop threads, close socket/session. Called at most once. */
    protected abstract void doClose(String reason);

    protected final HubFrame pollOutbound() { return outbound.poll(); }
    protected final HubFrame takeOutbound() throws InterruptedException {
        return outbound.take();
    }
    protected final int outboundDepth() { return outbound.size(); }
    protected final Hub hub()           { return hub; }
    protected final HubConfiguration config() { return config; }

    /**
     * Subclasses call this for each line received from the wire. Returns
     * {@code true} if the line was accepted and broadcast; {@code false} if
     * the peer has been closed (frame too large or already closed). Frame
     * size is the only inbound check — there is no rate limiter.
     */
    protected final boolean acceptInbound(String line) {
        if (!open.get()) return false;
        if (line.length() > config.maxFramePayloadBytes) {
            close("frame size exceeded (" + line.length()
                  + " > " + config.maxFramePayloadBytes + ")");
            return false;
        }
        stats.recordInboundFrame();
        hub.broadcast(new HubFrame(wireFormat, line), this);
        return true;
    }

    @Override
    public final void close(String reason) {
        if (!open.compareAndSet(true, false)) return;
        log.info("closing peer {} reason={} stats={{}}", remoteAddress, reason, stats.snapshot());
        // Drain the queue so any thread blocked in send() wakes promptly.
        // The drained frames are lost ONLY for this departing peer — the hub
        // has already delivered them to every other peer that is still open.
        outbound.clear();
        try {
            doClose(reason);
        } catch (Exception e) {
            log.warn("doClose threw for {}: {}", remoteAddress, e.toString());
        } finally {
            hub.unregister(this);
        }
    }
}
