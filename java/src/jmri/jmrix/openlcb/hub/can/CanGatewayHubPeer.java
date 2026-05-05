package jmri.jmrix.openlcb.hub.can;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import jmri.jmrix.can.CanListener;
import jmri.jmrix.can.CanMessage;
import jmri.jmrix.can.CanReply;
import jmri.jmrix.can.TrafficController;
import jmri.jmrix.can.adapters.gridconnect.GridConnectMessage;
import jmri.jmrix.can.adapters.gridconnect.GridConnectReply;
import jmri.jmrix.openlcb.hub.AbstractHubPeer;
import jmri.jmrix.openlcb.hub.Hub;
import jmri.jmrix.openlcb.hub.HubConfiguration;
import jmri.jmrix.openlcb.hub.HubFrame;
import jmri.jmrix.openlcb.hub.WireFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges the JMRI CAN bus into the hub as one more {@link
 * jmri.jmrix.openlcb.hub.HubPeer}. Inbound: JMRI's {@link CanListener}
 * callbacks reformat each frame as GridConnect ASCII and push it onto the
 * hub. Outbound: a writer thread drains the per-peer queue, parses each
 * GridConnect line back into a {@link CanReply}/{@link CanMessage}, and
 * hands it to the traffic controller plus the JMRI listener fan-out.
 *
 * <h2>Loopback prevention</h2>
 * The CAN driver echoes every outbound frame back through the listener
 * mechanism. The hub's source-skip handles peer-to-peer echoes inside the
 * hub itself, but it does not help here because by the time the echo
 * arrives the source identity has been lost — the frame is now coming from
 * the traffic controller, not from any hub peer.
 *
 * <p>Two identity-tracking lists ({@link #workingMessageSet}, {@link
 * #workingReplySet}) record every frame this gateway has just emitted.
 * When {@link #message(CanMessage)} or {@link #reply(CanReply)} fires for
 * the same frame, it is matched and removed rather than re-broadcast. This
 * is the same mechanism the legacy {@code HubPane} used; it has been
 * lifted into this peer unchanged.
 *
 * <p><strong>Driver-specific echo behaviour.</strong> Different CAN
 * adapters (LCC Pro USB, MERG CAN-USB, RR-CirKits, GridConnect-net) may
 * differ in how they surface the echo through {@link TrafficController}.
 * This peer is identical to the legacy logic and works for the adapters
 * the legacy hub supported, but each adapter requires its own validation
 * pass on real hardware.
 */
public class CanGatewayHubPeer extends AbstractHubPeer implements CanListener {

    private static final Logger log = LoggerFactory.getLogger(CanGatewayHubPeer.class);

    private final TrafficController trafficController;
    private final Thread writerThread;

    // Loopback tracking. Lists rather than sets to preserve the legacy
    // semantics (FIFO match-and-remove) and because hot-path size is 0-N
    // for small N where ArrayList beats hashing. Synchronized blocks guard
    // both the writer-thread mutations and the CanListener callback reads.
    private final List<CanMessage> workingMessageSet = new ArrayList<>();
    private final List<CanReply>   workingReplySet   = new ArrayList<>();

    public CanGatewayHubPeer(Hub hub, HubConfiguration config, TrafficController tc, String label) {
        super(hub, config, "can:" + label, WireFormat.GRIDCONNECT);
        this.trafficController = tc;
        this.writerThread = new Thread(this::writerLoop, "CanGatewayHubPeer-writer");
        this.writerThread.setDaemon(true);
    }

    /** Register on the hub and start listening to the CAN bus. */
    public void start() {
        hub().register(this);
        trafficController.addCanListener(this);
        writerThread.start();
    }

    @Override
    protected void onFrameQueued() {
        // Writer thread is always blocked in takeOutbound(); ArrayBlockingQueue
        // unblocks it on offer.
    }

    @Override
    protected void doClose(String reason) {
        try { trafficController.removeCanListener(this); } catch (RuntimeException ignored) {}
        writerThread.interrupt();
    }

    // ---------------------------------------------------------------- inbound (JMRI CAN -> hub)

    @Override
    public synchronized void message(CanMessage m) {
        if (workingMessageSet.contains(m)) {
            workingMessageSet.remove(m);
            log.trace("suppress forward of message {} from JMRI; WMS={} items", m, workingMessageSet.size());
            return;
        }
        GridConnectMessage gm = newGridConnectMessage(m);
        log.trace("forward message {}", gm);
        acceptInbound(stripTerminator(gm.toString()));
    }

    @Override
    public synchronized void reply(CanReply r) {
        if (workingReplySet.contains(r)) {
            workingReplySet.remove(r);
            log.trace("suppress forward of reply {} from JMRI; WRS={} items", r, workingReplySet.size());
            return;
        }
        GridConnectMessage gm = newGridConnectMessage(new CanMessage(r));
        log.trace("forward reply {} from JMRI", gm);
        acceptInbound(stripTerminator(gm.toString()));
    }

    /**
     * Factory hook for the GridConnect-encoded form of an outgoing CAN frame.
     * Subclasses (e.g. CBUS/MERG) override to swap in a wire-format-specific
     * subclass.
     */
    protected GridConnectMessage newGridConnectMessage(CanMessage m) {
        return new GridConnectMessage(m);
    }

    // ---------------------------------------------------------------- outbound (hub -> JMRI CAN)

    private void writerLoop() {
        try {
            while (isOpen()) {
                HubFrame frame = takeOutbound();
                long t0 = System.nanoTime();
                deliverToCan(frame.payload());
                stats().recordWriteLatency(System.nanoTime() - t0);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.warn("CAN gateway writer error: {}", e.toString());
        } finally {
            close("writer exit");
        }
    }

    private void deliverToCan(String line) {
        // Parse the GridConnect line into a CanReply
        GridConnectReply gcReply = newBlankReply();
        byte[] bytes = line.getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < line.length(); i++) {
            gcReply.setElement(i, bytes[i]);
        }
        CanReply workingReply = gcReply.createReply();
        workingReply.setSourceLetter("H");

        // Build the equivalent CanMessage to send out
        CanMessage outbound = new CanMessage(workingReply.getNumDataElements(), workingReply.getHeader());
        for (int i = 0; i < workingReply.getNumDataElements(); i++) {
            outbound.setElement(i, workingReply.getElement(i));
        }
        outbound.setExtended(workingReply.isExtended());
        outbound.setSourceLetter("H");

        // Mark both forms as "we just sent this" before handing them off,
        // so the resulting CanListener echo is recognised and dropped.
        synchronized (this) {
            workingReplySet.add(workingReply);
            workingMessageSet.add(outbound);
        }

        log.trace("CAN gateway emitting {}", workingReply);
        trafficController.sendCanMessage(outbound, null);
        trafficController.distributeOneReply(workingReply, this);
    }

    /**
     * Override point for tests or specialised gateways that need a custom
     * GridConnectReply subclass.
     */
    protected GridConnectReply newBlankReply() {
        return new GridConnectReply();
    }

    private static String stripTerminator(String line) {
        // GridConnectMessage.toString() includes the GridConnect ';' but no CR/LF;
        // defend anyway against trailing whitespace.
        int end = line.length();
        while (end > 0) {
            char c = line.charAt(end - 1);
            if (c == '\r' || c == '\n' || c == ' ' || c == '\t') { end--; continue; }
            break;
        }
        return end == line.length() ? line : line.substring(0, end);
    }
}
