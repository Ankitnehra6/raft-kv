package io.github.ankitnehra6.raftkv.log;

import io.github.ankitnehra6.raftkv.core.NodeId;
import java.util.Optional;

/**
 * The two values Raft requires to survive a restart, besides the log itself.
 *
 * <p>Losing either breaks safety rather than merely losing progress. A node that forgets
 * its term can accept a stale leader; a node that forgets its vote can vote twice in the
 * same term and help elect a second leader. Both are exactly the outcomes the algorithm
 * exists to prevent, which is why the paper insists these are written to disk *before* the
 * corresponding RPC is answered.
 *
 * @param currentTerm the highest term seen
 * @param votedFor who this node voted for in that term, or null
 */
public record PersistentState(long currentTerm, NodeId votedFor) {

    public static final PersistentState INITIAL = new PersistentState(0, null);

    public Optional<NodeId> votedForId() {
        return Optional.ofNullable(votedFor);
    }
}
