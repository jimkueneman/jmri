package jmri.jmrix.openlcb.hub;

/**
 * Pluggable authorization hook called by every listener at connection time.
 * v1 ships only {@link AllowAllAuthenticator}. The interface is in place so
 * a future PR can add token, basic-auth, or origin-allowlist implementations
 * without changing the listeners or peers.
 */
public interface HubAuthenticator {

    Decision authorize(PeerContext ctx);

    final class Decision {
        public final boolean allow;
        public final String reason;

        private Decision(boolean allow, String reason) {
            this.allow = allow;
            this.reason = reason;
        }

        public static Decision allow()              { return new Decision(true, "");      }
        public static Decision deny(String reason)  { return new Decision(false, reason); }
    }
}
