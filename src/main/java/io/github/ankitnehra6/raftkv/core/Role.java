package io.github.ankitnehra6.raftkv.core;

/** The three states a Raft server can be in. */
public enum Role {
    /** Passive: responds to leaders and candidates, and times out into an election. */
    FOLLOWER,
    /** Standing for election, collecting votes for its own term. */
    CANDIDATE,
    /** Elected: the only node that accepts client proposals. */
    LEADER
}
