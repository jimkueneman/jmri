package jmri.jmrix.openlcb.hub.websocket;

import java.util.concurrent.atomic.AtomicBoolean;

import jmri.jmrix.openlcb.hub.AbstractHubPeer;
import jmri.jmrix.openlcb.hub.Hub;
import jmri.jmrix.openlcb.hub.HubConfiguration;
import jmri.jmrix.openlcb.hub.HubFrame;
import jmri.jmrix.openlcb.hub.WireFormat;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One connected WebSocket client. Outbound writes are async via Jetty's
 * {@link WriteCallback} chain — at most one write in flight per peer at a
 * time, with the next frame pulled in {@link WriteCallback#writeSuccess}.
 * The hub broadcast thread never blocks on socket I/O for this peer.
 */
@WebSocket
public final class WebSocketHubPeer extends AbstractHubPeer {

    private static final Logger log = LoggerFactory.getLogger(WebSocketHubPeer.class);

    private volatile Session session;
    private final AtomicBoolean writeInFlight = new AtomicBoolean();

    public WebSocketHubPeer(Hub hub, HubConfiguration config, String remoteAddress) {
        super(hub, config, remoteAddress, WireFormat.GRIDCONNECT);
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
        this.session = session;
        session.setIdleTimeout(config().webSocketIdleTimeoutMs);
        hub().register(this);
        log.debug("ws onConnect {}", remoteAddress());
    }

    @OnWebSocketMessage
    public void onMessage(String message) {
        if (message == null) return;
        String stripped = stripTerminators(message);
        if (stripped.isEmpty()) return;
        acceptInbound(stripped);
    }

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        log.debug("ws onClose {} code={} reason={}", remoteAddress(), statusCode, reason);
        close("peer closed: " + statusCode + " " + (reason == null ? "" : reason));
    }

    @OnWebSocketError
    public void onError(Throwable cause) {
        log.debug("ws onError {}: {}", remoteAddress(), cause.toString());
        close("ws error: " + cause);
    }

    @Override
    protected void onFrameQueued() {
        if (session == null) return;          // not yet connected
        if (writeInFlight.compareAndSet(false, true)) {
            drainNext();
        }
    }

    private void drainNext() {
        HubFrame frame = pollOutbound();
        if (frame == null) {
            writeInFlight.set(false);
            // race: a frame may have been enqueued between poll() and clear
            if (outboundDepth() > 0 && writeInFlight.compareAndSet(false, true)) {
                drainNext();
            }
            return;
        }
        final long t0 = System.nanoTime();
        try {
            session.getRemote().sendString(frame.payload(), new WriteCallback() {
                @Override public void writeSuccess() {
                    stats().recordWriteLatency(System.nanoTime() - t0);
                    drainNext();
                }
                @Override public void writeFailed(Throwable x) {
                    writeInFlight.set(false);
                    close("ws write failed: " + x);
                }
            });
        } catch (RuntimeException e) {
            writeInFlight.set(false);
            close("ws write threw: " + e);
        }
    }

    @Override
    protected void doClose(String reason) {
        Session s = this.session;
        if (s != null && s.isOpen()) {
            try {
                s.close(StatusCode.NORMAL, truncateReason(reason));
            } catch (Exception ignored) {}
        }
    }

    private static String truncateReason(String reason) {
        // RFC 6455: close reason payload <= 123 bytes
        if (reason == null) return "";
        return reason.length() <= 120 ? reason : reason.substring(0, 120);
    }

    private static String stripTerminators(String s) {
        int end = s.length();
        while (end > 0) {
            char c = s.charAt(end - 1);
            if (c == '\r' || c == '\n' || c == ' ' || c == '\t') { end--; continue; }
            break;
        }
        return end == s.length() ? s : s.substring(0, end);
    }
}
