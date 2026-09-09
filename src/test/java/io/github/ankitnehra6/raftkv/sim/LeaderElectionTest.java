package io.github.ankitnehra6.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ankitnehra6.raftkv.core.RaftNode;
import io.github.ankitnehra6.raftkv.core.Role;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Leader election, driven entirely in logical time.
 *
 * <p>Not one {@code Thread.sleep} and not one real socket. Every test here is a fixed seed
 * away from being reproduced exactly.
 */
class LeaderElectionTest {

    private static final int GENEROUS_BUDGET = 200;

    @Test
    void aSingleNodeElectsItself() {
        SimulatedCluster cluster = new SimulatedCluster(1, 42);

        assertThat(cluster.tickUntilLeaderElected(GENEROUS_BUDGET)).isTrue();
        assertThat(cluster.node("n0").isLeader()).isTrue();
        // One vote is already a majority of one, so no RPC is needed.
        assertThat(cluster.network().deliveredCount()).isZero();
    }

    @ParameterizedTest(name = "cluster of {0}")
    @ValueSource(ints = {3, 5, 7})
    void electsExactlyOneLeader(int size) {
        SimulatedCluster cluster = new SimulatedCluster(size, 1234);

        assertThat(cluster.tickUntilLeaderElected(GENEROUS_BUDGET))
                .as("a healthy cluster of %d must elect a leader", size)
                .isTrue();

        assertThat(cluster.allLeaders()).hasSize(1);

        RaftNode leader = cluster.leader().orElseThrow();
        assertThat(leader.currentTerm()).isPositive();

        // Election completes the moment the candidate counts its majority, which is before
        // its first heartbeat has reached anyone. Followers learn who won only when that
        // AppendEntries arrives, so the cluster needs a few more ticks to settle before
        // asserting on their view of it.
        cluster.tick(20);

        // Everyone else must be a follower that recognises the same leader.
        cluster.nodes().stream()
                .filter(n -> !n.equals(leader))
                .forEach(
                        n -> {
                            assertThat(n.role()).isEqualTo(Role.FOLLOWER);
                            assertThat(n.leaderId()).contains(leader.id());
                            assertThat(n.currentTerm()).isEqualTo(leader.currentTerm());
                        });
    }

    /**
     * The core safety property: at most one leader may exist per term.
     *
     * <p>Checked on every tick rather than only at the end, because a split-brain that
     * resolves itself before the assertion would otherwise pass unnoticed — and a
     * transient double leader is still a double leader.
     */
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 7, 11, 42, 99, 12345, 987654321})
    void neverTwoLeadersInTheSameTerm(long seed) {
        SimulatedCluster cluster = new SimulatedCluster(5, seed);
        Map<Long, String> leaderPerTerm = new HashMap<>();

        for (int i = 0; i < 500; i++) {
            cluster.tick();

            for (RaftNode node : cluster.allLeaders()) {
                String existing = leaderPerTerm.putIfAbsent(node.currentTerm(), node.id().value());
                assertThat(existing)
                        .as("term %d had two leaders: %s and %s", node.currentTerm(), existing, node.id())
                        .satisfiesAnyOf(
                                e -> assertThat(e).isNull(),
                                e -> assertThat(e).isEqualTo(node.id().value()));
            }
        }
    }

    @Test
    void aLeaderKeepsLeadershipWhileHeartbeatsFlow() {
        SimulatedCluster cluster = new SimulatedCluster(5, 7);
        assertThat(cluster.tickUntilLeaderElected(GENEROUS_BUDGET)).isTrue();

        RaftNode leader = cluster.leader().orElseThrow();
        long term = leader.currentTerm();

        // Far longer than any election timeout. Heartbeats alone must hold the cluster.
        cluster.tick(500);

        assertThat(cluster.leader()).map(RaftNode::id).contains(leader.id());
        assertThat(leader.currentTerm())
                .as("a stable cluster must not churn through terms")
                .isEqualTo(term);
    }

    @Test
    void electsANewLeaderWhenTheCurrentOneCrashes() {
        SimulatedCluster cluster = new SimulatedCluster(5, 2024);
        assertThat(cluster.tickUntilLeaderElected(GENEROUS_BUDGET)).isTrue();

        RaftNode original = cluster.leader().orElseThrow();
        long originalTerm = original.currentTerm();

        cluster.crash(original.id().value());

        assertThat(cluster.tickUntil(GENEROUS_BUDGET, () -> cluster.leader().isPresent()))
                .as("the remaining four nodes are a majority and must elect a successor")
                .isTrue();

        RaftNode successor = cluster.leader().orElseThrow();
        assertThat(successor.id()).isNotEqualTo(original.id());
        assertThat(successor.currentTerm())
                .as("a new leadership requires a new term")
                .isGreaterThan(originalTerm);
    }

    /**
     * A minority cannot elect a leader, however long it tries. This is what stops a
     * partitioned-off subset from accepting writes that the majority never sees.
     */
    @Test
    void aMinorityPartitionCannotElectALeader() {
        SimulatedCluster cluster = new SimulatedCluster(5, 55);
        assertThat(cluster.tickUntilLeaderElected(GENEROUS_BUDGET)).isTrue();

        cluster.partition(List.of("n0", "n1"), List.of("n2", "n3", "n4"));
        cluster.tick(300);

        List<RaftNode> minority = List.of(cluster.node("n0"), cluster.node("n1"));
        assertThat(minority)
                .as("two of five is not a majority")
                .noneMatch(RaftNode::isLeader);
    }

    @Test
    void theMajoritySideElectsALeaderDuringAPartition() {
        SimulatedCluster cluster = new SimulatedCluster(5, 55);
        assertThat(cluster.tickUntilLeaderElected(GENEROUS_BUDGET)).isTrue();

        cluster.partition(List.of("n0", "n1"), List.of("n2", "n3", "n4"));

        List<RaftNode> majority = List.of(cluster.node("n2"), cluster.node("n3"), cluster.node("n4"));
        assertThat(cluster.tickUntil(GENEROUS_BUDGET, () -> majority.stream().anyMatch(RaftNode::isLeader)))
                .as("three of five is a majority and must make progress")
                .isTrue();
    }

    /**
     * A leader stranded in a minority must step down once it can see the higher term the
     * majority elected in its absence — otherwise the cluster really would have two.
     */
    @Test
    void anIsolatedLeaderStepsDownWhenThePartitionHeals() {
        SimulatedCluster cluster = new SimulatedCluster(5, 31);
        assertThat(cluster.tickUntilLeaderElected(GENEROUS_BUDGET)).isTrue();

        RaftNode original = cluster.leader().orElseThrow();
        long originalTerm = original.currentTerm();

        // Strand the leader alone against the other four.
        List<String> majority =
                cluster.nodes().stream()
                        .map(n -> n.id().value())
                        .filter(name -> !name.equals(original.id().value()))
                        .toList();
        cluster.partition(List.of(original.id().value()), majority);

        assertThat(
                        cluster.tickUntil(
                                GENEROUS_BUDGET,
                                () ->
                                        cluster.nodes().stream()
                                                .anyMatch(
                                                        n ->
                                                                n.isLeader()
                                                                        && n.currentTerm() > originalTerm)))
                .as("the majority must elect a leader in a higher term")
                .isTrue();

        cluster.heal();
        cluster.tick(100);

        assertThat(cluster.allLeaders())
                .as("after healing there must be exactly one leader again")
                .hasSize(1);
        assertThat(cluster.leader().orElseThrow().currentTerm()).isGreaterThan(originalTerm);
    }

    /**
     * Raft assumes nothing of the network beneath it, so elections have to converge even
     * when a fifth of messages vanish.
     */
    @Test
    void electsALeaderDespiteMessageLoss() {
        SimulatedCluster cluster = new SimulatedCluster(5, 808);
        cluster.setDropRate(0.2);

        assertThat(cluster.tickUntilLeaderElected(1000))
                .as("retries must converge even with 20%% packet loss")
                .isTrue();
        assertThat(cluster.network().droppedCount()).isPositive();
    }

    /** Variable delay reorders messages, which is where ordering assumptions break. */
    @Test
    void electsALeaderDespiteReorderedMessages() {
        SimulatedCluster cluster = new SimulatedCluster(5, 4242);
        cluster.setDelay(1, 6);

        assertThat(cluster.tickUntilLeaderElected(1000)).isTrue();
        assertThat(cluster.allLeaders()).hasSize(1);
    }

    /** The point of the harness: the same seed must produce byte-identical history. */
    @Test
    void runsAreReproducible() {
        assertThat(historyFor(9001)).isEqualTo(historyFor(9001));
        assertThat(historyFor(9001)).isNotEqualTo(historyFor(9002));
    }

    private static String historyFor(long seed) {
        SimulatedCluster cluster = new SimulatedCluster(5, seed);
        cluster.setDropRate(0.1);
        cluster.setDelay(1, 4);

        StringBuilder history = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            cluster.tick();
            cluster.nodes()
                    .forEach(n -> history.append(n.id()).append(n.role()).append(n.currentTerm()).append('|'));
        }
        return history.toString();
    }
}
