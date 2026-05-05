/**
 * JMRI's OpenLCB hub. Per-peer async I/O with end-to-end blocking
 * backpressure replaces the older third-party {@code org.openlcb.hub.Hub}.
 *
 * <p>Three peer types share one {@link jmri.jmrix.openlcb.hub.HubPeer}
 * abstraction:
 * <ul>
 *   <li>{@link jmri.jmrix.openlcb.hub.tcp.TcpHubPeer} — raw TCP/GridConnect</li>
 *   <li>{@link jmri.jmrix.openlcb.hub.websocket.WebSocketHubPeer} — Jetty WS</li>
 *   <li>{@link jmri.jmrix.openlcb.hub.can.CanGatewayHubPeer} — JMRI CAN bus</li>
 * </ul>
 *
 * <p>The hub never drops frames or disconnects peers for slowness. A slow
 * peer paces the bus to its drain rate via blocking {@link
 * jmri.jmrix.openlcb.hub.HubPeer#send}. See {@code OPENLCB_HUB_CHANGE_REQUEST.md}
 * for design rationale and validation.
 */
package jmri.jmrix.openlcb.hub;
