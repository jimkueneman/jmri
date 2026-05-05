package jmri.jmrix.openlcb.hub;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Central peer registry and broadcast point. Holds zero owned threads:
 * {@link #broadcast} runs on the caller's thread (typically a peer's reader)
 * and dispatches to every other peer via {@link HubPeer#send}.
 *
 * Note: under OpenLCB's reliability premise, {@link HubPeer#send} blocks on
 * a full outbound queue rather than dropping. A persistently slow peer will
 * therefore stall the broadcast loop until its queue drains — this is the
 * intended end-to-end backpressure path. Transient slowness is absorbed by
 * the per-peer queue (default 16,384 frames) and is invisible to the bus.
 */
public final class Hub {

    private static final Logger log = LoggerFactory.getLogger(Hub.class);

    private final CopyOnWriteArrayList<HubPeer> peers = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<FrameListener> frameListeners = new CopyOnWriteArrayList<>();

    /**
     * Optional observer of every frame that crosses the hub. Invoked from the
     * broadcasting thread; implementations must be cheap and non-blocking.
     */
    @FunctionalInterface
    public interface FrameListener {
        void onFrame(HubFrame frame, HubPeer source);
    }

    public void register(HubPeer peer) {
        peers.add(peer);
        log.info("peer registered {} ({}); total={}",
            peer.remoteAddress(), peer.wireFormat(), peers.size());
    }

    public void unregister(HubPeer peer) {
        if (peers.remove(peer)) {
            log.info("peer unregistered {}; total={}",
                peer.remoteAddress(), peers.size());
        }
    }

    public void addFrameListener(FrameListener l)    { frameListeners.add(l); }
    public void removeFrameListener(FrameListener l) { frameListeners.remove(l); }

    /**
     * Fan out a frame to every peer except the source. Each {@code send} call
     * may block briefly on a full per-peer queue (backpressure, not error).
     * Whole-bus latency at steady state is sub-millisecond; under congestion
     * it tracks the slowest current peer.
     */
    public void broadcast(HubFrame frame, HubPeer source) {
        for (FrameListener l : frameListeners) {
            try { l.onFrame(frame, source); }
            catch (RuntimeException e) { log.warn("frame listener threw: {}", e.toString()); }
        }
        for (HubPeer p : peers) {
            if (p == source) continue;
            // v1 only handles same-format pass-through. Cross-format gateway
            // logic will live outside this method (see § 5.8 of the change
            // request).
            if (p.wireFormat() != frame.format()) continue;
            try {
                p.send(frame);
                p.stats().recordOutboundFrame();
            } catch (RuntimeException e) {
                log.warn("send threw for {}: {}", p.remoteAddress(), e.toString());
                p.close("send error: " + e);
            }
        }
    }

    public Collection<HubPeer> peers() {
        return Collections.unmodifiableList(peers);
    }

    public int peerCount() { return peers.size(); }

    /** Close every registered peer. Useful for shutdown and test teardown. */
    public void closeAll(String reason) {
        for (HubPeer p : peers) {
            try { p.close(reason); } catch (RuntimeException ignored) {}
        }
    }
}
