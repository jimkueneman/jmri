package jmri.jmrix.openlcb.hub;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;

/**
 * Information available to {@link HubAuthenticator} when deciding whether
 * to admit a connection. Built by each listener for its transport.
 */
public final class PeerContext {

    public enum Transport { TCP, WEBSOCKET }

    private final Transport transport;
    private final InetSocketAddress remote;
    private final Map<String, String> headers;     // empty for TCP
    private final String requestedSubprotocol;     // null for TCP

    public PeerContext(Transport transport,
                       InetSocketAddress remote,
                       Map<String, String> headers,
                       String requestedSubprotocol) {
        this.transport = Objects.requireNonNull(transport);
        this.remote = remote;
        this.headers = headers == null ? Map.of() : Map.copyOf(headers);
        this.requestedSubprotocol = requestedSubprotocol;
    }

    public Transport transport()              { return transport; }
    public InetSocketAddress remote()         { return remote; }
    public Map<String, String> headers()      { return headers; }
    public String requestedSubprotocol()      { return requestedSubprotocol; }
}
