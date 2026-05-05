package jmri.jmrix.openlcb.hub;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class HubConfigurationTest {

    @Test
    void defaultsAreSensible() {
        HubConfiguration c = HubConfiguration.defaults();
        assertTrue(c.outboundQueueDepth >= 16_384,
            "default queue depth must absorb 500-node startup burst, got " + c.outboundQueueDepth);
        assertTrue(c.tcpPort > 0 && c.tcpPort < 65536);
        assertEquals("/lcc/hub", c.webSocketPath);
        assertEquals("openlcb-gc", c.webSocketSubprotocol);
    }

    @Test
    void builderOverridesIndividualFields() {
        HubConfiguration c = HubConfiguration.builder()
            .outboundQueueDepth(99)
            .tcpPort(40000)
            .build();
        assertEquals(99, c.outboundQueueDepth);
        assertEquals(40000, c.tcpPort);
        // unset fields keep defaults
        assertEquals("/lcc/hub", c.webSocketPath);
    }

    @Test
    void defaultTcpPortMatchesLegacyHub() {
        // The new hub replaces org.openlcb.hub.Hub, which used 12021. Keeping
        // the default preserves ZeroConf compatibility for existing clients.
        assertEquals(12021, HubConfiguration.defaults().tcpPort);
    }
}
