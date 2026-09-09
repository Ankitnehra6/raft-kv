package io.github.ankitnehra6.raftkv.sim;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import io.github.ankitnehra6.raftkv.core.NodeId;
import io.github.ankitnehra6.raftkv.core.RaftNode;
import io.github.ankitnehra6.raftkv.linearizability.History;
import io.github.ankitnehra6.raftkv.linearizability.Operation;
import io.github.ankitnehra6.raftkv.statemachine.Command;
import io.github.ankitnehra6.raftkv.statemachine.KeyValueStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Drives a simulated cluster as a set of clients, recording everything they observe.
 *
 * <p>Each node keeps its own {@link KeyValueStore}, fed from that node's committed entries,
 * exactly as a real server's apply loop would. Clients propose through the leader and are
 * answered when their entry is applied.
 *
 * <p>The history it produces contains only what a client could see from outside — request
 * sent, answer received — which is the only evidence a linearizability claim may rest on.
 */
final class StoreDriver {

    /** A request that has been proposed and is waiting for its entry to be applied. */
    private record InFlight(
            long index, byte[] command, History.Pending handle, Operation.Kind kind) {}

    private final SimulatedCluster cluster;
    private final Map<NodeId, KeyValueStore> stores = new LinkedHashMap<>();
    private final Map<NodeId, Integer> consumed = new LinkedHashMap<>();
    private final History history = new History();
    private final List<InFlight> inFlight = new ArrayList<>();

    StoreDriver(SimulatedCluster cluster) {
        this.cluster = cluster;
        cluster.nodes()
                .forEach(
                        n -> {
                            stores.put(n.id(), new KeyValueStore());
                            consumed.put(n.id(), 0);
                        });
    }

    SimulatedCluster cluster() {
        return cluster;
    }

    History history() {
        return history;
    }

    KeyValueStore storeAt(NodeId id) {
        return stores.get(id);
    }

    /** Advances the cluster and applies whatever each node committed. */
    void tick(int count) {
        for (int i = 0; i < count; i++) {
            cluster.tick();
            applyNewlyCommitted();
        }
    }

    private void applyNewlyCommitted() {
        for (RaftNode node : cluster.nodes()) {
            // A node that was sent a snapshot must have its state machine replaced before
            // any further entries are applied on top of it.
            node.takeSnapshotToRestore()
                    .ifPresent(
                            snapshot -> {
                                stores.get(node.id())
                                        .restore(snapshot.data(), snapshot.lastIncludedIndex());
                                // Everything the cluster recorded as applied for this node
                                // is now covered by the snapshot.
                                consumed.put(node.id(), cluster.appliedAt(node.id()).size());
                            });
        }

        for (RaftNode node : cluster.nodes()) {
            List<LogEntry> all = cluster.appliedAt(node.id());
            int already = consumed.get(node.id());
            if (all.size() <= already) {
                continue;
            }
            consumed.put(node.id(), all.size());

            for (LogEntry entry : all.subList(already, all.size())) {
                Optional<String> readResult = stores.get(node.id()).apply(entry);
                settle(entry, readResult);
            }
        }
    }

    /**
     * Completes any in-flight request this entry answers.
     *
     * <p>Matched on index <em>and</em> command bytes. Index alone is not enough: a proposal
     * made to a leader that then lost its leadership can be overwritten at that index by a
     * different entry, and completing the client's request against someone else's write
     * would fabricate a result the store never gave.
     */
    private void settle(LogEntry entry, Optional<String> readResult) {
        inFlight.removeIf(
                pending -> {
                    if (pending.index() != entry.index()) {
                        return false;
                    }
                    if (!Arrays.equals(pending.command(), entry.command())) {
                        // Overwritten by a different leader's entry: the client never got
                        // an answer, so it stays in the history as pending.
                        pending.handle().abandon();
                        return true;
                    }
                    pending.handle()
                            .complete(
                                    pending.kind() == Operation.Kind.GET
                                            ? readResult.orElse(null)
                                            : null,
                                    cluster.currentTick());
                    return true;
                });
    }

    // --- client operations -------------------------------------------------------

    /** Issues a write. Returns false if there is no leader to accept it right now. */
    boolean put(int process, String key, String value) {
        return propose(process, Operation.Kind.PUT, key, value, new Command.Put(key, value));
    }

    boolean delete(int process, String key) {
        return propose(process, Operation.Kind.DELETE, key, null, new Command.Delete(key));
    }

    /**
     * Issues a read.
     *
     * <p>Replicated through the log like a write, so the value returned is the state at the
     * read's position in the log rather than whatever the leader's map happens to hold when
     * the client asks.
     */
    boolean get(int process, String key) {
        return propose(process, Operation.Kind.GET, key, null, new Command.Get(key));
    }

    private boolean propose(
            int process, Operation.Kind kind, String key, String value, Command command) {

        Optional<RaftNode> leader = cluster.leader();
        if (leader.isEmpty()) {
            return false;
        }

        byte[] encoded = Command.encode(command);
        long invokedAt = cluster.currentTick();

        Optional<Long> index = leader.get().propose(encoded);
        if (index.isEmpty()) {
            return false;
        }

        History.Pending handle = history.invoke(kind, process, key, value, invokedAt);
        inFlight.add(new InFlight(index.get(), encoded, handle, kind));
        return true;
    }

    /** Marks every still-unanswered request as pending, at the end of a run. */
    void abandonOutstanding() {
        inFlight.forEach(p -> p.handle().abandon());
        inFlight.clear();
    }

    int outstandingCount() {
        return inFlight.size();
    }

    /**
     * Compacts every node's log up to what it has applied.
     *
     * <p>Driver-initiated because only the driver knows the state machine: Raft cannot
     * serialise the result of commands it treats as opaque bytes.
     */
    void compactAll() {
        for (RaftNode node : cluster.nodes()) {
            KeyValueStore store = stores.get(node.id());
            long applied = store.lastAppliedIndex();
            if (applied > node.log().snapshotIndex() && applied <= node.lastApplied()) {
                node.compact(applied, store.snapshotBytes());
            }
        }
    }
}
