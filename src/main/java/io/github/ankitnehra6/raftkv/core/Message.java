package io.github.ankitnehra6.raftkv.core;

import java.util.List;

/**
 * Everything nodes say to each other.
 *
 * <p>A sealed hierarchy so the compiler enforces that every message is handled — adding a
 * new RPC without wiring it up becomes a compile error rather than a silently dropped
 * message that shows up as a stalled cluster three weeks later.
 *
 * <p>Every message carries its sender's term. That single field drives most of Raft: a
 * higher term anywhere means step down, a lower term means the sender is stale.
 */
public sealed interface Message {

    NodeId from();

    NodeId to();

    long term();

    /**
     * Sent by a candidate soliciting a vote.
     *
     * @param lastLogIndex the candidate's last log index
     * @param lastLogTerm the candidate's last log term. Together with the index this is
     *     the up-to-date check: a voter refuses a candidate whose log is behind its own,
     *     which is what guarantees an elected leader holds every committed entry.
     */
    record RequestVote(
            NodeId from, NodeId to, long term, long lastLogIndex, long lastLogTerm)
            implements Message {}

    record RequestVoteResponse(NodeId from, NodeId to, long term, boolean voteGranted)
            implements Message {}

    /**
     * Sent by a leader to replicate entries, and as a heartbeat when empty.
     *
     * @param prevLogIndex index of the entry immediately preceding {@code entries}
     * @param prevLogTerm term of that entry. The follower refuses unless it has a matching
     *     entry there, which is the induction step that keeps logs identical.
     * @param entries the entries to store, possibly empty for a heartbeat
     * @param leaderCommit the leader's commit index, so followers learn what is safe to
     *     apply
     */
    record AppendEntries(
            NodeId from,
            NodeId to,
            long term,
            long prevLogIndex,
            long prevLogTerm,
            List<LogEntry> entries,
            long leaderCommit)
            implements Message {

        public AppendEntries {
            entries = List.copyOf(entries);
        }

        public boolean isHeartbeat() {
            return entries.isEmpty();
        }
    }

    /**
     * @param success whether the consistency check passed
     * @param matchIndex on success, the highest index the follower now has from this
     *     leader. Returned explicitly rather than inferred by the leader from what it sent,
     *     because a retransmitted or reordered response would otherwise advance matchIndex
     *     to the wrong place.
     * @param conflictIndex on failure, the first index the leader should try next. Lets
     *     the leader skip a whole divergent suffix in one round trip instead of walking
     *     back one index at a time.
     */
    record AppendEntriesResponse(
            NodeId from,
            NodeId to,
            long term,
            boolean success,
            long matchIndex,
            long conflictIndex)
            implements Message {}
}
