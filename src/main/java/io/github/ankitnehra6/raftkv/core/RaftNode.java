package io.github.ankitnehra6.raftkv.core;

import io.github.ankitnehra6.raftkv.log.InMemoryLogStore;
import io.github.ankitnehra6.raftkv.log.LogStore;
import io.github.ankitnehra6.raftkv.log.PersistentState;
import io.github.ankitnehra6.raftkv.log.RaftLog;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.random.RandomGenerator;

/**
 * One Raft server, as a pure state machine.
 *
 * <p>This class has no threads, no sockets, no files and no wall clock. It accepts inputs
 * — {@link #tick()}, {@link #receive}, {@link #propose} — mutates its state, and queues
 * outputs for a driver to collect with {@link #drainOutbound()} and
 * {@link #drainCommitted()}. Whatever moves the bytes, be it a real network or a simulated
 * one, lives outside.
 *
 * <p>That separation is the whole reason this design was chosen. Consensus bugs are
 * overwhelmingly bugs of timing and ordering: a vote arriving after a term change, a
 * response from a leadership that has already ended, two nodes both believing they lead.
 * With time and message delivery injected, an entire five-node cluster runs single-threaded
 * inside a unit test, a failing seed reproduces exactly, and there is not one
 * {@code Thread.sleep} in the suite. Testing the same algorithm through real sockets and
 * real clocks means waiting for those interleavings to occur by luck.
 *
 * <p>Not safe for concurrent use, and deliberately so — the driver owns the thread.
 */
public class RaftNode {

    private final NodeId id;
    private final Set<NodeId> peers;
    private final RaftConfig config;
    private final RandomGenerator random;
    private final RaftLog log;

    // --- persistent state (would survive a restart) ---
    private long currentTerm;
    private NodeId votedFor;

    // --- volatile state ---
    private Role role = Role.FOLLOWER;
    private long commitIndex;
    private long lastApplied;
    private NodeId leaderId;

    // --- leader state, reset on election ---
    private final Map<NodeId, Long> nextIndex = new HashMap<>();
    private final Map<NodeId, Long> matchIndex = new HashMap<>();

    // --- election bookkeeping ---
    private final Set<NodeId> votesReceived = new LinkedHashSet<>();
    private int ticksSinceHeardFromLeader;
    private int ticksSinceHeartbeat;
    private int electionTimeout;

    // --- outputs, drained by the driver ---
    private final List<Message> outbound = new ArrayList<>();
    private final List<LogEntry> committed = new ArrayList<>();

    public RaftNode(NodeId id, Set<NodeId> peers, RaftConfig config, RandomGenerator random) {
        this(id, peers, config, random, new InMemoryLogStore());
    }

    /**
     * Opens a node over a store, recovering whatever state it already holds.
     *
     * <p>A restarted node must come back with the term it had reached, the vote it had
     * cast and the entries it had accepted. Starting blank would let it vote a second time
     * in a term it has already voted in, which is one of the two ways a cluster ends up
     * with two leaders.
     */
    public RaftNode(
            NodeId id,
            Set<NodeId> peers,
            RaftConfig config,
            RandomGenerator random,
            LogStore store) {
        if (peers.contains(id)) {
            throw new IllegalArgumentException("peers must not include this node: " + id);
        }
        this.id = id;
        this.peers = Set.copyOf(peers);
        this.config = config;
        this.random = random;
        this.log = new RaftLog(store);

        PersistentState recovered = store.loadState();
        this.currentTerm = recovered.currentTerm();
        this.votedFor = recovered.votedFor();

        resetElectionTimer();
    }

    /**
     * Writes term and vote to stable storage.
     *
     * <p>Called before any reply that depends on them. The paper is explicit about the
     * ordering: a node that answers a vote request and only then persists the vote can
     * crash in between, come back with no memory of it, and vote again in the same term.
     */
    private void persistState() {
        log.store().saveState(new PersistentState(currentTerm, votedFor));
    }

    // --- inputs ------------------------------------------------------------------

    /**
     * Advances logical time by one tick.
     *
     * <p>A leader sends heartbeats on schedule; everyone else counts down to an election.
     */
    public void tick() {
        if (role == Role.LEADER) {
            ticksSinceHeartbeat++;
            if (ticksSinceHeartbeat >= config.heartbeatIntervalTicks()) {
                ticksSinceHeartbeat = 0;
                peers.forEach(this::sendAppendEntries);
            }
            return;
        }

        ticksSinceHeardFromLeader++;
        if (ticksSinceHeardFromLeader >= electionTimeout) {
            startElection();
        }
    }

    /** Handles a message from another node. */
    public void receive(Message message) {
        if (!message.to().equals(id)) {
            throw new IllegalArgumentException("message for %s delivered to %s".formatted(message.to(), id));
        }

        // Rule for all servers: any message carrying a higher term means this node is
        // stale. It reverts to follower before doing anything else, which is what stops
        // two leaders from coexisting once they can see each other.
        if (message.term() > currentTerm) {
            stepDown(message.term());
        }

        switch (message) {
            case Message.RequestVote m -> onRequestVote(m);
            case Message.RequestVoteResponse m -> onRequestVoteResponse(m);
            case Message.AppendEntries m -> onAppendEntries(m);
            case Message.AppendEntriesResponse m -> onAppendEntriesResponse(m);
        }
    }

    /**
     * Submits a command for replication.
     *
     * @return the index it will occupy, or empty if this node is not the leader. Returning
     *     empty rather than throwing because "not the leader" is routine — it happens on
     *     every election — and the caller's job is to redirect, not to handle an exception.
     */
    public Optional<Long> propose(byte[] command) {
        if (role != Role.LEADER) {
            return Optional.empty();
        }
        LogEntry entry = new LogEntry(currentTerm, log.lastIndex() + 1, command);
        log.append(entry);

        // Replicate immediately rather than waiting for the next heartbeat, so a quiet
        // cluster does not add up to a heartbeat interval of latency to every write.
        peers.forEach(this::sendAppendEntries);

        // A single-node cluster has a majority of one, so the entry is already committed.
        advanceCommitIndex();
        return Optional.of(entry.index());
    }

    // --- outputs -----------------------------------------------------------------

    /** Takes the messages queued since the last call. */
    public List<Message> drainOutbound() {
        List<Message> drained = List.copyOf(outbound);
        outbound.clear();
        return drained;
    }

    /** Takes the entries committed since the last call, in log order. */
    public List<LogEntry> drainCommitted() {
        List<LogEntry> drained = List.copyOf(committed);
        committed.clear();
        return drained;
    }

    // --- RequestVote --------------------------------------------------------------

    private void onRequestVote(Message.RequestVote m) {
        boolean granted = shouldGrantVote(m);

        if (granted) {
            votedFor = m.from();
            // Only a granted vote resets the timer. Resetting on every request would let
            // a node that keeps failing elections indefinitely suppress a healthy one.
            resetElectionTimer();
        }

        // Durable before the reply leaves. A vote the candidate counts but this node
        // forgets is exactly how a term ends up with two leaders.
        persistState();
        send(new Message.RequestVoteResponse(id, m.from(), currentTerm, granted));
    }

    private boolean shouldGrantVote(Message.RequestVote m) {
        if (m.term() < currentTerm) {
            return false;
        }
        // At most one vote per term. Granting again to the same candidate is safe and
        // necessary, because the first response may have been lost.
        if (votedFor != null && !votedFor.equals(m.from())) {
            return false;
        }
        return isAtLeastAsUpToDateAsOurs(m.lastLogIndex(), m.lastLogTerm());
    }

    /**
     * The election restriction from §5.4.1: a voter refuses any candidate whose log is
     * behind its own.
     *
     * <p>This is the single rule that makes committed entries durable. A majority must
     * vote for a leader, and a majority must have stored any committed entry, so those two
     * majorities overlap in at least one node — and that node will refuse a candidate
     * missing the entry. Drop this check and a leader can be elected that silently loses
     * acknowledged writes.
     */
    private boolean isAtLeastAsUpToDateAsOurs(long candidateLastIndex, long candidateLastTerm) {
        long ourLastTerm = log.lastTerm();
        if (candidateLastTerm != ourLastTerm) {
            return candidateLastTerm > ourLastTerm;
        }
        return candidateLastIndex >= log.lastIndex();
    }

    private void onRequestVoteResponse(Message.RequestVoteResponse m) {
        // A response from an election this node has already left, or already lost, is
        // meaningless. Without this check a late vote could "elect" a node that stepped
        // down several terms ago.
        if (role != Role.CANDIDATE || m.term() != currentTerm) {
            return;
        }
        if (!m.voteGranted()) {
            return;
        }

        votesReceived.add(m.from());
        if (isMajority(votesReceived.size())) {
            becomeLeader();
        }
    }

    // --- AppendEntries -------------------------------------------------------------

    private void onAppendEntries(Message.AppendEntries m) {
        if (m.term() < currentTerm) {
            // A stale leader. The reply carries this node's higher term, which tells the
            // sender to step down.
            send(new Message.AppendEntriesResponse(id, m.from(), currentTerm, false, 0, 0));
            return;
        }

        // A valid leader for this term exists, so a candidate abandons its election.
        role = Role.FOLLOWER;
        leaderId = m.from();
        resetElectionTimer();

        if (!log.matches(m.prevLogIndex(), m.prevLogTerm())) {
            send(
                    new Message.AppendEntriesResponse(
                            id, m.from(), currentTerm, false, 0, conflictHintFor(m.prevLogIndex())));
            return;
        }

        long lastNewIndex = log.appendFrom(m.prevLogIndex(), m.entries());

        if (m.leaderCommit() > commitIndex) {
            // Clamped to what this node actually has: the leader may be ahead, and
            // committing an index this log has not received yet would apply nothing and
            // then skip it forever.
            commitIndex = Math.min(m.leaderCommit(), Math.max(lastNewIndex, log.lastIndex()));
            applyCommitted();
        }

        send(new Message.AppendEntriesResponse(id, m.from(), currentTerm, true, lastNewIndex, 0));
    }

    /**
     * Tells the leader where to resume after a failed consistency check.
     *
     * <p>Without a hint the leader walks back one index per round trip, which after a long
     * partition costs one round trip per divergent entry. Naming the first index of the
     * conflicting term skips the whole run at once.
     */
    private long conflictHintFor(long prevLogIndex) {
        if (prevLogIndex > log.lastIndex()) {
            // The log is simply too short; resume at its end.
            return log.lastIndex() + 1;
        }

        long conflictingTerm = log.termAt(prevLogIndex);
        long index = prevLogIndex;
        while (index > 1 && log.termAt(index - 1) == conflictingTerm) {
            index--;
        }
        return index;
    }

    private void onAppendEntriesResponse(Message.AppendEntriesResponse m) {
        // Responses addressed to a leadership that has already ended must be ignored, or
        // a stale success could advance matchIndex under a new leader's bookkeeping.
        if (role != Role.LEADER || m.term() != currentTerm) {
            return;
        }

        if (m.success()) {
            // Taken from the response rather than from what was sent: a reordered or
            // duplicated reply would otherwise move matchIndex to the wrong place.
            matchIndex.put(m.from(), Math.max(matchIndex.getOrDefault(m.from(), 0L), m.matchIndex()));
            nextIndex.put(m.from(), matchIndex.get(m.from()) + 1);
            advanceCommitIndex();
            return;
        }

        long next = Math.max(1, m.conflictIndex());
        nextIndex.put(m.from(), next);
        sendAppendEntries(m.from()); // retry immediately from the hinted position
    }

    // --- leadership ----------------------------------------------------------------

    private void startElection() {
        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = id;
        leaderId = null;
        votesReceived.clear();
        votesReceived.add(id);
        resetElectionTimer();
        persistState();

        // A single-node cluster elects itself: one vote is already a majority.
        if (isMajority(votesReceived.size())) {
            becomeLeader();
            return;
        }

        for (NodeId peer : peers) {
            send(new Message.RequestVote(id, peer, currentTerm, log.lastIndex(), log.lastTerm()));
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        ticksSinceHeartbeat = 0;

        nextIndex.clear();
        matchIndex.clear();
        for (NodeId peer : peers) {
            // Optimistically assume followers match, and let the consistency check walk
            // back if they do not.
            nextIndex.put(peer, log.lastIndex() + 1);
            matchIndex.put(peer, 0L);
        }

        // A no-op entry in the new term, per §5.4.2. A leader may not commit an entry from
        // an earlier term by counting replicas — doing so can un-commit it if the leader
        // then fails. Committing one entry of its own term makes every earlier entry
        // committed by implication, so this is what unblocks the backlog safely.
        log.append(LogEntry.noop(currentTerm, log.lastIndex() + 1));

        peers.forEach(this::sendAppendEntries);
        advanceCommitIndex(); // single-node clusters commit the no-op immediately
    }

    private void stepDown(long newTerm) {
        currentTerm = newTerm;
        role = Role.FOLLOWER;
        votedFor = null;
        leaderId = null;
        votesReceived.clear();
        resetElectionTimer();
        persistState();
    }

    private void sendAppendEntries(NodeId peer) {
        long next = nextIndex.getOrDefault(peer, log.lastIndex() + 1);
        long prevIndex = next - 1;
        long prevTerm = prevIndex == 0 ? 0 : log.termAt(prevIndex);

        send(
                new Message.AppendEntries(
                        id, peer, currentTerm, prevIndex, prevTerm, log.from(next), commitIndex));
    }

    /**
     * Advances the commit index to the highest entry stored on a majority.
     *
     * <p>The term check is the subtle half of §5.4.2: only entries from the leader's
     * <em>current</em> term may be committed by counting replicas. An entry from an earlier
     * term can be present on a majority and still be overwritten by a future leader, so
     * counting it as committed would be a lie. It becomes committed indirectly, once an
     * entry above it in the current term commits.
     */
    private void advanceCommitIndex() {
        for (long candidate = log.lastIndex(); candidate > commitIndex; candidate--) {
            if (log.termAt(candidate) != currentTerm) {
                continue;
            }

            int replicas = 1; // this node stores it
            for (NodeId peer : peers) {
                if (matchIndex.getOrDefault(peer, 0L) >= candidate) {
                    replicas++;
                }
            }

            if (isMajority(replicas)) {
                commitIndex = candidate;
                applyCommitted();
                return;
            }
        }
    }

    /** Queues every newly committed entry for the state machine, in order. */
    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            log.entryAt(lastApplied).ifPresent(committed::add);
        }
    }

    private boolean isMajority(int count) {
        return count * 2 > clusterSize();
    }

    private int clusterSize() {
        return peers.size() + 1;
    }

    private void resetElectionTimer() {
        ticksSinceHeardFromLeader = 0;
        // Randomised per election. A fixed timeout makes every follower stand at the same
        // instant, split the vote, and repeat.
        electionTimeout =
                random.nextInt(
                        config.electionTimeoutMinTicks(), config.electionTimeoutMaxTicks() + 1);
    }

    private void send(Message message) {
        outbound.add(message);
    }

    // --- inspection ----------------------------------------------------------------

    public NodeId id() {
        return id;
    }

    public Role role() {
        return role;
    }

    public long currentTerm() {
        return currentTerm;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    public Optional<NodeId> leaderId() {
        return Optional.ofNullable(leaderId);
    }

    public Optional<NodeId> votedFor() {
        return Optional.ofNullable(votedFor);
    }

    public RaftLog log() {
        return log;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    /** A snapshot of leader replication progress, for assertions and debugging. */
    public Map<NodeId, Long> matchIndexes() {
        return Map.copyOf(matchIndex);
    }

    /** Peers this node knows about, excluding itself. */
    public Set<NodeId> peers() {
        return new HashSet<>(peers);
    }

    @Override
    public String toString() {
        return "%s[%s term=%d commit=%d lastIndex=%d]"
                .formatted(id, role, currentTerm, commitIndex, log.lastIndex());
    }
}
