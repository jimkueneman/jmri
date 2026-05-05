package jmri.jmrix.openlcb.hub;

import java.util.Objects;

/**
 * Immutable value carried through the hub. Holds the wire-format payload
 * (a GridConnect line for v1) plus a format tag so the same hub can later
 * carry OpenLCB-TCP binary messages without API change.
 */
public final class HubFrame {

    private final WireFormat format;
    private final String payload;

    public HubFrame(WireFormat format, String payload) {
        this.format = Objects.requireNonNull(format);
        this.payload = Objects.requireNonNull(payload);
    }

    public static HubFrame gridConnect(String line) {
        return new HubFrame(WireFormat.GRIDCONNECT, line);
    }

    public WireFormat format() { return format; }
    public String payload()   { return payload; }
    public int length()       { return payload.length(); }

    @Override
    public String toString() {
        return format + ":" + payload;
    }
}
