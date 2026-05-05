package jmri.jmrix.openlcb.hub.itest;

import java.net.URI;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

/**
 * In-process WebSocket client used by integration scenarios. Tracks
 * received-frame count. Reuses a static shared {@link WebSocketClient} to
 * keep thread overhead low when many peers are spawned.
 */
public final class WebSocketTestPeer implements AutoCloseable {

    private static final WebSocketClient SHARED = new WebSocketClient();
    static {
        try {
            SHARED.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try { SHARED.stop(); } catch (Exception ignored) {}
            }));
        } catch (Exception e) {
            throw new IllegalStateException("WebSocketClient failed to start", e);
        }
    }

    private final Endpoint endpoint;

    public WebSocketTestPeer(URI uri, String subprotocol, String tag) throws Exception {
        endpoint = new Endpoint(tag);
        ClientUpgradeRequest req = new ClientUpgradeRequest();
        req.setSubProtocols(subprotocol);
        Future<Session> f = SHARED.connect(endpoint, uri, req);
        endpoint.session = f.get(5, TimeUnit.SECONDS);
    }

    public void send(String frame) {
        Session s = endpoint.session;
        if (s == null || !s.isOpen()) return;
        try {
            s.getRemote().sendString(frame);
        } catch (Exception ignored) {
            // session closed mid-send; ignore
        }
    }

    public long received()    { return endpoint.rx.get(); }
    public boolean isOpen()   { Session s = endpoint.session; return s != null && s.isOpen(); }
    public Integer closeCode(){ return endpoint.closeCode; }
    public String closeReason() { return endpoint.closeReason; }

    /**
     * Slow this peer's read path to simulate a slow consumer. The delay is
     * applied inside {@code @OnWebSocketMessage}, which holds the Jetty
     * client-side read for that connection and propagates backpressure
     * through the WS session to the hub.
     */
    public void setReadDelayMs(int ms) { endpoint.readDelayMs = ms; }

    @Override
    public void close() {
        Session s = endpoint.session;
        if (s != null && s.isOpen()) s.close();
    }

    @WebSocket
    public static class Endpoint {
        private final AtomicLong rx = new AtomicLong();
        private final String tag;
        private volatile Session session;
        private volatile Integer closeCode;
        private volatile String closeReason;
        volatile int readDelayMs;

        Endpoint(String tag) { this.tag = tag; }

        @OnWebSocketConnect public void onConnect(Session s) { this.session = s; }
        @OnWebSocketMessage public void onMessage(String msg) {
            rx.incrementAndGet();
            int delay = readDelayMs;
            if (delay > 0) {
                try { Thread.sleep(delay); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }
        @OnWebSocketClose   public void onClose(int code, String reason) {
            closeCode = code;
            closeReason = reason == null ? "" : reason;
        }
    }
}
