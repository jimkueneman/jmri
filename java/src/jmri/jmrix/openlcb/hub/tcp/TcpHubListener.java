package jmri.jmrix.openlcb.hub.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import jmri.jmrix.openlcb.hub.Hub;
import jmri.jmrix.openlcb.hub.HubAuthenticator;
import jmri.jmrix.openlcb.hub.HubConfiguration;
import jmri.jmrix.openlcb.hub.PeerContext;
import jmri.util.zeroconf.ZeroConfService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server socket on {@link HubConfiguration#tcpPort}. Spawns one
 * {@link TcpHubPeer} per accept and registers it with the hub. Optionally
 * publishes a {@code _openlcb-can._tcp.local.} ZeroConf advertisement on
 * the listening port (preserving the legacy JMRI hub's discovery behaviour).
 */
public final class TcpHubListener {

    public static final String OPENLCB_ZEROCONF_TYPE = "_openlcb-can._tcp.local.";

    private static final Logger log = LoggerFactory.getLogger(TcpHubListener.class);

    private final Hub hub;
    private final HubConfiguration config;
    private final HubAuthenticator authenticator;
    private final boolean advertiseZeroConf;
    private final String zeroConfServiceType;
    private ServerSocket server;
    private Thread acceptThread;
    private volatile boolean running;
    private ZeroConfService zeroConfService;

    public TcpHubListener(Hub hub, HubConfiguration config, HubAuthenticator auth) {
        this(hub, config, auth, true, OPENLCB_ZEROCONF_TYPE);
    }

    public TcpHubListener(Hub hub,
                          HubConfiguration config,
                          HubAuthenticator auth,
                          boolean advertiseZeroConf) {
        this(hub, config, auth, advertiseZeroConf, OPENLCB_ZEROCONF_TYPE);
    }

    public TcpHubListener(Hub hub,
                          HubConfiguration config,
                          HubAuthenticator auth,
                          boolean advertiseZeroConf,
                          String zeroConfServiceType) {
        this.hub = hub;
        this.config = config;
        this.authenticator = auth;
        this.advertiseZeroConf = advertiseZeroConf;
        this.zeroConfServiceType = zeroConfServiceType;
    }

    public void start() throws IOException {
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(config.tcpPort));
        running = true;
        acceptThread = new Thread(this::acceptLoop, "TcpHubListener-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
        log.info("TCP listener up on port {}", config.tcpPort);
        if (advertiseZeroConf) {
            try {
                zeroConfService = ZeroConfService.create(zeroConfServiceType, config.tcpPort);
                zeroConfService.publish();
                log.info("ZeroConf published {} on port {}", zeroConfServiceType, config.tcpPort);
            } catch (RuntimeException e) {
                log.warn("ZeroConf publish failed: {}", e.toString());
            }
        }
    }

    public void stop() {
        running = false;
        if (zeroConfService != null) {
            try { zeroConfService.stop(); } catch (RuntimeException ignored) {}
            zeroConfService = null;
        }
        try { if (server != null) server.close(); } catch (IOException ignored) {}
    }

    public int port() { return config.tcpPort; }

    private void acceptLoop() {
        while (running) {
            Socket socket;
            try {
                socket = server.accept();
            } catch (IOException e) {
                if (running) log.warn("accept failed: {}", e.toString());
                continue;
            }

            PeerContext ctx = new PeerContext(
                PeerContext.Transport.TCP,
                (InetSocketAddress) socket.getRemoteSocketAddress(),
                Map.of(),
                null);

            HubAuthenticator.Decision decision = authenticator.authorize(ctx);
            if (!decision.allow) {
                log.info("rejecting TCP {} reason={}", socket.getRemoteSocketAddress(), decision.reason);
                try { socket.close(); } catch (IOException ignored) {}
                continue;
            }

            try {
                socket.setTcpNoDelay(true);
                TcpHubPeer peer = new TcpHubPeer(hub, config, socket);
                hub.register(peer);
                peer.start();
            } catch (IOException e) {
                log.warn("setup failed for {}: {}", socket.getRemoteSocketAddress(), e.toString());
                try { socket.close(); } catch (IOException ignored) {}
            }
        }
    }
}
