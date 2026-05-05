package jmri.jmrix.openlcb.hub;

/**
 * A single member of the hub's broadcast set. Implementations bridge the
 * hub abstraction to a specific transport (raw TCP, WebSocket, JMRI CAN bus,
 * etc.). Every implementation MUST honour the contract that {@link #send}
 * never blocks the caller — incoming frames go onto an internal bounded
 * queue and are drained asynchronously by the implementation.
 */
public interface HubPeer {

    /** Stable, human-readable identifier shown in logs and stats. */
    String remoteAddress();

    /** Wire format this peer reads and writes on its transport. */
    WireFormat wireFormat();

    /**
     * Hub-to-peer delivery. Under the OpenLCB reliability premise this
     * blocks the caller when the per-peer outbound queue is full so
     * backpressure propagates back to the original source — frames are
     * never dropped.
     */
    void send(HubFrame frame);

    /** Initiate close. Idempotent. */
    void close(String reason);

    /** True until close has fully completed. */
    boolean isOpen();

    PeerStats stats();
}
