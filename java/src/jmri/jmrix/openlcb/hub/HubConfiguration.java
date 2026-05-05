package jmri.jmrix.openlcb.hub;

/**
 * Tunable parameters for the hub. All fields have sensible defaults; tests
 * may override individually.
 *
 * The hub never drops frames or disconnects peers for slowness — OpenLCB
 * requires every frame to be delivered to every connected peer. There is
 * therefore no inbound rate limiter and no slow-consumer drop policy in
 * this configuration.
 */
public final class HubConfiguration {

    public final int outboundQueueDepth;
    public final int  maxFramePayloadBytes;
    public final int  tcpPort;
    public final String webSocketPath;
    public final String webSocketSubprotocol;
    public final long webSocketIdleTimeoutMs;
    /**
     * If true, the TCP writer appends CR+LF after every frame. If false,
     * just LF. GridConnect tools differ in what they require — the original
     * JMRI hub exposed this as a user preference.
     */
    public final boolean sendCrLf;
    /**
     * If true, the TCP reader requires a CR or LF terminator and discards
     * un-terminated input. If false, any whitespace ends a frame.
     */
    public final boolean requireLineTermination;

    private HubConfiguration(Builder b) {
        this.outboundQueueDepth     = b.outboundQueueDepth;
        this.maxFramePayloadBytes   = b.maxFramePayloadBytes;
        this.tcpPort                = b.tcpPort;
        this.webSocketPath          = b.webSocketPath;
        this.webSocketSubprotocol   = b.webSocketSubprotocol;
        this.webSocketIdleTimeoutMs = b.webSocketIdleTimeoutMs;
        this.sendCrLf               = b.sendCrLf;
        this.requireLineTermination = b.requireLineTermination;
    }

    public static HubConfiguration defaults() { return new Builder().build(); }
    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        // Sized to absorb the worst-case 500-node OpenLCB startup burst
        // (~16,000 frames in a sub-second window) without any peer's queue
        // overflowing under normal conditions. See § 4 of the change request.
        private int  outboundQueueDepth     = 16_384;
        private int  maxFramePayloadBytes   = 64;
        private int  tcpPort                = 12021;
        private String webSocketPath        = "/lcc/hub";
        private String webSocketSubprotocol = "openlcb-gc";
        private long webSocketIdleTimeoutMs = 0;
        private boolean sendCrLf            = true;
        private boolean requireLineTermination = false;

        public Builder outboundQueueDepth(int v)     { this.outboundQueueDepth = v;     return this; }
        public Builder maxFramePayloadBytes(int v)   { this.maxFramePayloadBytes = v;   return this; }
        public Builder tcpPort(int v)                { this.tcpPort = v;                return this; }
        public Builder webSocketPath(String v)       { this.webSocketPath = v;          return this; }
        public Builder webSocketSubprotocol(String v){ this.webSocketSubprotocol = v;   return this; }
        public Builder webSocketIdleTimeoutMs(long v){ this.webSocketIdleTimeoutMs = v; return this; }
        public Builder sendCrLf(boolean v)           { this.sendCrLf = v;               return this; }
        public Builder requireLineTermination(boolean v) { this.requireLineTermination = v; return this; }

        public HubConfiguration build() { return new HubConfiguration(this); }
    }
}
