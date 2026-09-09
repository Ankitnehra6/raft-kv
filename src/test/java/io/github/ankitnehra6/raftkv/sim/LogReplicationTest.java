package io.github.ankitnehra6.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import io.github.ankitnehra6.raftkv.core.RaftNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Log replication and commitment, under a healthy network and a hostile one. */
class LogReplicationTest {

    private static final int BUDGET = 300;

    private static byte[] command(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> commandsOf(List<LogEntry> entries) {
        return entries.stream()
                .filter(e -> !e.isNoop())
                .map(e -> new String(e.command(), StandardCharsets.UTF_8))
                .toList();
    }

    private static SimulatedCluster electedCluster(int size, long seed) {
        SimulatedCluster cluster = new SimulatedCluster(size, seed);
        assertThat(cluster.tickUntilLeaderElected(BUDGET)).isTrue();
        return cluster;
    }

    @Test
    void onlyTheLeaderAcceptsProposals() {
        SimulatedCluster cluster = electedCluster(3, 11);
        RaftNode leader = cluster.leader().orElseThrow();

        assertThat(leader.propose(command("accepted"))).isPresent();

        cluster.nodes().stream()
                .filter(n -> !n.equals(leader))
                .forEach(
                        follower ->
                                assertThat(follower.propose(command("rejected")))
                                        .as("%s is not the leader", follower.id())
                                        .isEmpty());
    }

    @Test
    void replicatesAnEntryToEveryFollower() {
        SimulatedCluster cluster = electedCluster(5, 22);
        RaftNode leader = cluster.leader().orElseThrow();

        long index = leader.propose(command("hello")).orElseThrow();
        cluster.tick(50);

        for (RaftNode node : cluster.nodes()) {
            assertThat(node.log().entryAt(index))
                    .as("%s should have the entry at index %d", node.id(), index)
                    .isPresent()
                    .get()
                    .extracting(e -> new String(e.command(), StandardCharsets.UTF_8))
                    .isEqualTo("hello");
        }
    }

    @Test
    void commitsAndAppliesOnEveryNode() {
        SimulatedCluster cluster = electedCluster(5, 33);
        RaftNode leader = cluster.leader().orElseThrow();

        leader.propose(command("a"));
        leader.propose(command("b"));
        leader.propose(command("c"));
        cluster.tick(80);

        for (RaftNode node : cluster.nodes()) {
            assertThat(commandsOf(cluster.appliedAt(node.id())))
                    .as("%s must apply every command, in order", node.id())
                    .containsExactly("a", "b", "c");
        }
    }

    /**
     * Applied order must be identical everywhere. This is the property the whole algorithm
     * exists to provide: replicas that apply the same commands in the same order end up in
     * the same state.
     */
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 5, 17, 64, 512, 4096})
    void everyNodeAppliesTheSameSequence(long seed) {
        SimulatedCluster cluster = electedCluster(5, seed);

        for (int round = 0; round < 10; round++) {
            cluster.leader().ifPresent(l -> l.propose(command("cmd")));
            cluster.tick(20);
        }
        cluster.tick(200);

        List<List<LogEntry>> logs =
                cluster.nodes().stream()
                        .map(n -> cluster.appliedAt(n.id()))
                        .toList();

        // Nodes may be at different points in the sequence, but wherever two have both
        // applied an index, they must agree on it — divergence at any position is a
        // correctness failure, not a timing artefact.
        int shortest = logs.stream().mapToInt(List::size).min().orElse(0);
        assertThat(shortest).as("every node should have applied something").isPositive();

        for (int position = 0; position < shortest; position++) {
            final int at = position;
            List<LogEntry> entries = logs.stream().map(l -> l.get(at)).toList();
            assertThat(entries)
                    .as("position %d must be identical across all nodes", at)
                    .allMatch(e -> e.equals(entries.getFirst()));
        }
    }

    /**
     * A leader cut off from the majority cannot commit. Its followers may even store the
     * entry, but without a majority acknowledging it, it must never be reported as
     * committed — a later leader is entitled to overwrite it.
     */
    @Test
    void aMinorityLeaderCannotCommit() {
        SimulatedCluster cluster = electedCluster(5, 77);
        RaftNode leader = cluster.leader().orElseThrow();

        List<String> others =
                cluster.nodes().stream()
                        .map(n -> n.id().value())
                        .filter(name -> !name.equals(leader.id().value()))
                        .toList();

        // Leader plus one follower on one side; the other three on the far side.
        cluster.partition(List.of(leader.id().value(), others.getFirst()), others.subList(1, 3 + 1));

        long commitBefore = leader.commitIndex();
        leader.propose(command("doomed"));
        cluster.tick(200);

        assertThat(leader.commitIndex())
                .as("two of five cannot commit anything")
                .isEqualTo(commitBefore);
    }

    /**
     * The uncommitted write of a deposed leader must be replaced, not merged. Raft
     * guarantees the leader's log wins; the follower's divergent suffix is discarded.
     */
    @Test
    void aDivergentSuffixIsOverwrittenByTheNewLeader() {
        SimulatedCluster cluster = electedCluster(5, 123);
        RaftNode original = cluster.leader().orElseThrow();

        // Commit something everyone agrees on first.
        original.propose(command("agreed"));
        cluster.tick(50);
        long agreedCommit = original.commitIndex();

        // Isolate the leader and let it accept a write nobody else will ever see.
        List<String> majority =
                cluster.nodes().stream()
                        .map(n -> n.id().value())
                        .filter(name -> !name.equals(original.id().value()))
                        .toList();
        cluster.partition(List.of(original.id().value()), majority);

        original.propose(command("orphaned"));
        cluster.tick(50);

        assertThat(original.log().lastIndex())
                .as("the isolated leader still appended it locally")
                .isGreaterThan(agreedCommit);

        // The majority elects a new leader and makes progress without it.
        assertThat(
                        cluster.tickUntil(
                                BUDGET,
                                () ->
                                        majority.stream()
                                                .map(cluster::node)
                                                .anyMatch(RaftNode::isLeader)))
                .isTrue();

        RaftNode successor =
                majority.stream().map(cluster::node).filter(RaftNode::isLeader).findFirst().orElseThrow();
        successor.propose(command("replacement"));
        cluster.tick(50);

        cluster.heal();
        cluster.tick(300);

        assertThat(commandsOf(original.log().entries()))
                .as("the orphaned write must be gone, replaced by the majority's history")
                .doesNotContain("orphaned")
                .contains("agreed", "replacement");
    }

    /** After any amount of chaos, once the network is healthy again the logs must agree. */
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {3, 8, 21, 55, 144})
    void logsConvergeAfterHealing(long seed) {
        SimulatedCluster cluster = electedCluster(5, seed);

        for (int round = 0; round < 6; round++) {
            cluster.leader().ifPresent(l -> l.propose(command("round" + Math.random())));
            cluster.tick(15);

            // Rotate a partition through the cluster, isolating a different pair each time.
            int a = round % 5;
            int b = (round + 1) % 5;
            cluster.partition(
                    List.of("n" + a, "n" + b),
                    List.of("n" + (round + 2) % 5, "n" + (round + 3) % 5, "n" + (round + 4) % 5));
            cluster.tick(40);
            cluster.heal();
            cluster.tick(40);
        }

        cluster.tick(500);

        long commitIndex =
                cluster.nodes().stream().mapToLong(RaftNode::commitIndex).max().orElseThrow();
        assertThat(commitIndex).as("the cluster must have made progress").isPositive();

        // Every node's log must agree at every committed index.
        for (long index = 1; index <= commitIndex; index++) {
            final long i = index;
            List<Optional<LogEntry>> atIndex =
                    cluster.nodes().stream().map(n -> n.log().entryAt(i)).toList();

            LogEntry reference = atIndex.getFirst().orElseThrow();
            assertThat(atIndex)
                    .as("committed index %d must be identical on every node", index)
                    .allMatch(e -> e.isPresent() && e.get().equals(reference));
        }
    }

    @Test
    void replicatesDespiteMessageLoss() {
        SimulatedCluster cluster = new SimulatedCluster(5, 999);
        cluster.setDropRate(0.15);
        assertThat(cluster.tickUntilLeaderElected(1000)).isTrue();

        RaftNode leader = cluster.leader().orElseThrow();
        for (int i = 0; i < 5; i++) {
            leader.propose(command("lossy" + i));
        }

        assertThat(cluster.tickUntil(2000, () -> leader.commitIndex() >= leader.log().lastIndex()))
                .as("retries must eventually get every entry committed")
                .isTrue();
    }

    /**
     * A follower that misses a long run of entries must catch up. The conflict hint is what
     * keeps this from costing one round trip per missing entry.
     */
    @Test
    void aRejoiningFollowerCatchesUp() {
        SimulatedCluster cluster = electedCluster(5, 246);
        RaftNode leader = cluster.leader().orElseThrow();

        String laggard =
                cluster.nodes().stream()
                        .map(n -> n.id().value())
                        .filter(name -> !name.equals(leader.id().value()))
                        .findFirst()
                        .orElseThrow();

        cluster.crash(laggard);

        for (int i = 0; i < 30; i++) {
            leader.propose(command("while-you-were-out-" + i));
        }
        cluster.tick(100);
        assertThat(leader.commitIndex()).isGreaterThan(20);

        cluster.restart(laggard);
        long tickOnRestart = cluster.currentTick();

        assertThat(
                        cluster.tickUntil(
                                BUDGET,
                                () -> cluster.node(laggard).log().lastIndex() >= leader.log().lastIndex()))
                .as("the returning follower must be brought fully up to date")
                .isTrue();

        assertThat(cluster.currentTick() - tickOnRestart)
                .as("catch-up should take a handful of round trips, not one per entry")
                .isLessThan(60);
    }
}
