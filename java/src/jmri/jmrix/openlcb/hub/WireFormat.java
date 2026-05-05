package jmri.jmrix.openlcb.hub;

/**
 * Wire-format identifier carried on every {@link HubFrame}.
 * v1 only ever produces or accepts {@link #GRIDCONNECT}; {@link #OPENLCB_TCP}
 * is reserved for the future binary OpenLCB-TCP Transfer protocol.
 */
public enum WireFormat {
    GRIDCONNECT,
    OPENLCB_TCP
}
