package jmri.jmrix.openlcb.hub.websocket;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;

import jmri.InstanceManager;
import jmri.jmrix.openlcb.hub.AllowAllAuthenticator;
import jmri.jmrix.openlcb.hub.Hub;
import jmri.jmrix.openlcb.hub.HubAuthenticator;
import jmri.jmrix.openlcb.hub.HubConfiguration;
import jmri.jmrix.openlcb.hub.PeerContext;

import org.eclipse.jetty.websocket.api.WebSocketPolicy;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.openide.util.lookup.ServiceProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebSocket endpoint at {@code /lcc}, auto-registered on JMRI's WebServer
 * via {@link ServiceProvider}. Looks up the live {@link Hub} from
 * {@link InstanceManager} on upgrade — when no hub is running (HubPane not
 * open), upgrade returns HTTP 503.
 *
 * Negotiates the {@code openlcb-gc} subprotocol; reserves {@code openlcb-tcp}
 * for the future binary OpenLCB-TCP Transfer protocol.
 */
@WebServlet(name = "WebSocketHubServlet", urlPatterns = {"/lcc/hub"})
@ServiceProvider(service = HttpServlet.class)
public final class WebSocketHubServlet extends WebSocketServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(WebSocketHubServlet.class);

    public WebSocketHubServlet() { /* no-arg ctor for ServiceProvider */ }

    @Override
    public void configure(WebSocketServletFactory factory) {
        WebSocketPolicy policy = factory.getPolicy();
        // Inbound size guard. GridConnect frames are <40 bytes; allow 256.
        policy.setMaxTextMessageSize(256);
        HubConfiguration cfg = lookupConfig();
        policy.setIdleTimeout(cfg.webSocketIdleTimeoutMs > 0 ? cfg.webSocketIdleTimeoutMs : 0);

        factory.setCreator(new HubWebSocketCreator());
    }

    static HubConfiguration lookupConfig() {
        HubConfiguration c = InstanceManager.getNullableDefault(HubConfiguration.class);
        return c != null ? c : HubConfiguration.defaults();
    }

    static HubAuthenticator lookupAuth() {
        HubAuthenticator a = InstanceManager.getNullableDefault(HubAuthenticator.class);
        return a != null ? a : new AllowAllAuthenticator();
    }

    private static final class HubWebSocketCreator implements WebSocketCreator {
        @Override
        public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
            Hub hub = InstanceManager.getNullableDefault(Hub.class);
            if (hub == null) {
                log.info("WS upgrade rejected — no hub running (HubPane not open)");
                resp.setStatusCode(503);
                return null;
            }
            HubConfiguration cfg = lookupConfig();
            HubAuthenticator auth = lookupAuth();

            // Subprotocol negotiation. Client must list our subprotocol;
            // future binary `openlcb-tcp` is reserved and rejected here.
            List<String> requested = req.getSubProtocols();
            String chosen = null;
            for (String p : requested) {
                if (cfg.webSocketSubprotocol.equalsIgnoreCase(p)) {
                    chosen = cfg.webSocketSubprotocol;
                    break;
                }
            }
            if (chosen == null && !requested.isEmpty()) {
                log.info("rejecting WS upgrade {} requested={} supported={}",
                    req.getRemoteSocketAddress(), requested, cfg.webSocketSubprotocol);
                resp.setStatusCode(400);
                return null;
            }
            if (chosen != null) resp.setAcceptedSubProtocol(chosen);

            Map<String, String> headers = new HashMap<>();
            req.getHeaders().forEach((k, v) -> {
                if (v != null && !v.isEmpty()) headers.put(k, v.get(0));
            });

            PeerContext ctx = new PeerContext(
                PeerContext.Transport.WEBSOCKET,
                (InetSocketAddress) req.getRemoteSocketAddress(),
                headers,
                chosen);

            HubAuthenticator.Decision decision = auth.authorize(ctx);
            if (!decision.allow) {
                log.info("rejecting WS {} reason={}", req.getRemoteSocketAddress(), decision.reason);
                resp.setStatusCode(403);
                return null;
            }

            String remote = req.getRemoteSocketAddress() == null
                ? "unknown" : req.getRemoteSocketAddress().toString();
            return new WebSocketHubPeer(hub, cfg, remote);
        }
    }
}
