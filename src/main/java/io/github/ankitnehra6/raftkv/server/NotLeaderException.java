package io.github.ankitnehra6.raftkv.server;

/**
 * Raised when a request reaches a node that cannot serve it.
 *
 * <p>Carries the leader's id when one is known, so a client can redirect rather than poll.
 * This is a routine outcome — it happens on every election — which is why the leader hint
 * matters more than the failure itself.
 */
public class NotLeaderException extends RuntimeException {

    private final String leaderHint;

    public NotLeaderException(String leaderHint) {
        super(leaderHint == null || leaderHint.isEmpty()
                ? "not the leader, and no leader is currently known"
                : "not the leader; try " + leaderHint);
        this.leaderHint = leaderHint == null ? "" : leaderHint;
    }

    /** The current leader's id, or empty if this node does not know one. */
    public String leaderHint() {
        return leaderHint;
    }
}
