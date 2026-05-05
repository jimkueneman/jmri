package jmri.jmrix.openlcb.hub;

/** Default authenticator: admits every connection. */
public final class AllowAllAuthenticator implements HubAuthenticator {
    @Override
    public Decision authorize(PeerContext ctx) {
        return Decision.allow();
    }
}
