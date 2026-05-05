package jmri.jmrix.openlcb.hub.itest;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;

import jmri.InstanceManager;
import jmri.jmrix.openlcb.hub.AllowAllAuthenticator;
import jmri.jmrix.openlcb.hub.Hub;
import jmri.jmrix.openlcb.hub.HubAuthenticator;
import jmri.jmrix.openlcb.hub.HubConfiguration;
import jmri.jmrix.openlcb.hub.tcp.TcpHubListener;
import jmri.jmrix.openlcb.hub.websocket.WebSocketHubServlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

/**
 * Spins up a fresh hub plus TCP and WebSocket listeners on ephemeral ports
 * so each scenario starts clean. Uses the same servlet-discovery path as
 * production: the {@link WebSocketHubServlet} (no-arg constructor) is mounted
 * on a Jetty context and looks up the {@link Hub} from {@link InstanceManager}.
 *
 * <p>Each {@code TestHarness} replaces the {@link InstanceManager} defaults
 * for {@link Hub}, {@link HubConfiguration}, and {@link HubAuthenticator}.
 * Scenarios run serially so this is safe.
 */
public final class TestHarness implements AutoCloseable {

    public final Hub hub;
    public final HubConfiguration config;
    public final int wsPort;
    private final Server server;
    private final TcpHubListener tcpListener;

    public TestHarness(HubConfiguration override) throws Exception {
        // Pick free ports
        int wsPort  = freePort();
        int tcpPort = freePort();
        this.wsPort = wsPort;
        this.config = HubConfiguration.builder()
            .outboundQueueDepth(override.outboundQueueDepth)
            .maxFramePayloadBytes(override.maxFramePayloadBytes)
            .tcpPort(tcpPort)
            .webSocketPath(override.webSocketPath)
            .webSocketSubprotocol(override.webSocketSubprotocol)
            .webSocketIdleTimeoutMs(override.webSocketIdleTimeoutMs)
            .sendCrLf(override.sendCrLf)
            .requireLineTermination(override.requireLineTermination)
            .build();
        this.hub = new Hub();
        HubAuthenticator auth = new AllowAllAuthenticator();

        // Publish into InstanceManager so the auto-instantiated servlet finds them
        InstanceManager.setDefault(Hub.class, hub);
        InstanceManager.setDefault(HubConfiguration.class, config);
        InstanceManager.setDefault(HubAuthenticator.class, auth);

        this.server = new Server(wsPort);
        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        server.setHandler(ctx);
        ctx.addServlet(WebSocketHubServlet.class, config.webSocketPath);
        server.start();

        this.tcpListener = new TcpHubListener(hub, config, auth, /*advertiseZeroConf=*/ false);
        tcpListener.start();
    }

    public TcpTestPeer connectTcp(String tag) throws IOException {
        return new TcpTestPeer("localhost", config.tcpPort, tag);
    }

    public WebSocketTestPeer connectWebSocket(String tag) throws Exception {
        URI uri = URI.create("ws://localhost:" + wsPort + config.webSocketPath);
        return new WebSocketTestPeer(uri, config.webSocketSubprotocol, tag);
    }

    public void stop() {
        try { hub.closeAll("harness teardown"); } catch (RuntimeException ignored) {}
        try { tcpListener.stop(); } catch (RuntimeException ignored) {}
        try { server.stop(); } catch (Exception ignored) {}
    }

    @Override public void close() { stop(); }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }
}
