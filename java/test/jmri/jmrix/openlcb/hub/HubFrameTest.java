package jmri.jmrix.openlcb.hub;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class HubFrameTest {

    @Test
    void gridConnectFactoryTagsFormat() {
        HubFrame f = HubFrame.gridConnect(":X19490AAAN0102030405060708;");
        assertEquals(WireFormat.GRIDCONNECT, f.format());
        assertEquals(":X19490AAAN0102030405060708;", f.payload());
    }

    @Test
    void rejectsNullArguments() {
        assertThrows(NullPointerException.class, () -> new HubFrame(null, "x"));
        assertThrows(NullPointerException.class, () -> new HubFrame(WireFormat.GRIDCONNECT, null));
    }

    @Test
    void lengthMatchesPayload() {
        HubFrame f = HubFrame.gridConnect(":X12345N00;");
        assertEquals(":X12345N00;".length(), f.length());
    }
}
