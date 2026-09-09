package io.github.ankitnehra6.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ankitnehra6.raftkv.core.RaftNode;
import io.github.ankitnehra6.raftkv.core.Role;
import io.github.ankitnehra6.raftkv.linearizability.LinearizabilityChecker;
import io.github.ankitnehra6.raftkv.log.PersistentState;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * What a node must remember across a restart.
 *
 * <p>A restart here genuinely rebuilds the {@link RaftNode} from its store: term, vote and
 * log survive, everything volatile does not. Modelling it any more gently — keeping the
 * in-memory object alive and merely pausing it — would let a recovery bug pass unnoticed,
 * which is the usual reason these tests are worthless when they exist at all.
 */
class CrashRecoveryTest {

    private static final int BUDGET = 400;

    private static byte[] command(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The safety-critical one. A node that votes, crashes and comes back must remember the
     * vote — otherwise it can vote a second time in the same term and help elect a second
     * leader, which is one of exactly two ways Raft's core guarantee is broken.
     */
    @Test
    void aRestartedNodeRemembersItsVote() {
        SimulatedCluster cluster = new SimulatedCluster(3, 5);
        assertThat(cluster.tickUntilLeaderElected(BUDGET)).isTrue();
        cluster.tick(20);

        // Find a follower that has actually voted.
        RaftNode voter =
                cluster.nodes().stream()
                        .filter(n -> n.votedFor().isPresent() && !n.isLeader())
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("no follower recorded a vote"));

        long termBefore = voter.currentTerm();
        var votedForBefore = voter.votedFor().orElseThrow();

        cluster.crash(voter.id().value());
        cluster.restart(voter.id().value());

        RaftNode recovered = cluster.node(voter.id());

        assertThat(recovered.currentTerm())
                .as("a forgotten term lets a stale leader be accepted")
                .isEqualTo(termBefore);
        assertThat(recovered.votedFor())
                .as("a forgotten vote lets the same term be voted in twice")
                .contains(votedForBefore);
    }

    /** Volatile state must not survive; only the three persistent fields may. */
    @Test
    void aRestartedLeaderComesBackAsAFollower() {
        SimulatedCluster cluster = new SimulatedCluster(3, 9);
        assertThat(cluster.tickUntilLeaderElected(BUDGET)).isTrue();
        cluster.tick(20);

        RaftNode leader = cluster.leader().orElseThrow();
        String id = leader.id().value();

        cluster.crash(id);
        cluster.restart(id);

        assertThat(cluster.node(id).role())
                .as("leadership is volatile; a restarted node must re-earn it")
                .isEqualTo(Role.FOLLOWER);
        assertThat(cluster.node(id).commitIndex())
                .as("commit index is volatile and rebuilt from the leader")
                .isZero();
    }

    @Test
    void aRestartedNodeRecoversItsLog() {
        SimulatedCluster cluster = new SimulatedCluster(5, 13);
        assertThat(cluster.tickUntilLeaderElected(BUDGET)).isTrue();
        cluster.tick(20);

        RaftNode leader = cluster.leader().orElseThrow();
        for (int i = 0; i < 10; i++) {
            leader.propose(command("entry-" + i));
        }
        cluster.tick(80);

        String follower =
                cluster.nodes().stream()
                        .map(n -> n.id().value())
                        .filter(name -> !name.equals(leader.id().value()))
                        .findFirst()
                        .orElseThrow();

        long lastIndexBefore = cluster.node(follower).log().lastIndex();
        assertThat(lastIndexBefore).isGreaterThan(5);

        cluster.crash(follower);
        cluster.restart(follower);

        assertThat(cluster.node(follower).log().lastIndex())
                .as("the log is on disk and must come back")
                .isEqualTo(lastIndexBefore);
    }

    /** Everything committed before a total outage must still be there afterwards. */
    @Test
    void committedDataSurvivesAWholeClusterRestart() {
        SimulatedCluster cluster = new SimulatedCluster(3, 21);
        assertThat(cluster.tickUntilLeaderElected(BUDGET)).isTrue();
        cluster.tick(20);

        RaftNode leader = cluster.leader().orElseThrow();
        for (int i = 0; i < 8; i++) {
            leader.propose(command("durable-" + i));
        }
        cluster.tick(100);

        long committed = leader.commitIndex();
        assertThat(committed).isGreaterThan(5);

        // Everything goes down at once.
        for (int i = 0; i < 3; i++) {
            cluster.crash("n" + i);
        }
        for (int i = 0; i < 3; i++) {
            cluster.restart("n" + i);
        }

        // Every node's stored log must still contain the committed prefix.
        for (int i = 0; i < 3; i++) {
            assertThat(cluster.storeAt("n" + i).lastIndex())
                    .as("n%d lost data across the outage", i)
                    .isGreaterThanOrEqualTo(committed);
        }

        // And the cluster must be able to elect and make progress again.
        assertThat(cluster.tickUntilLeaderElected(BUDGET))
                .as("a recovered cluster must be able to elect")
                .isTrue();
        cluster.tick(50);

        RaftNode newLeader = cluster.leader().orElseThrow();
        assertThat(newLeader.log().lastIndex())
                .as("the new leader must hold everything that was committed")
                .isGreaterThanOrEqualTo(committed);
    }

    @Test
    void storedStateMatchesTheNodesView() {
        SimulatedCluster cluster = new SimulatedCluster(3, 33);
        assertThat(cluster.tickUntilLeaderElected(BUDGET)).isTrue();
        cluster.tick(40);

        for (RaftNode node : cluster.nodes()) {
            PersistentState stored = cluster.storeAt(node.id().value()).loadState();

            assertThat(stored.currentTerm())
                    .as("%s persisted a different term than it holds", node.id())
                    .isEqualTo(node.currentTerm());
            assertThat(stored.votedForId())
                    .as("%s persisted a different vote than it holds", node.id())
                    .isEqualTo(node.votedFor());
        }
    }

    /**
     * The end-to-end claim: clients still observe a linearizable store when nodes are being
     * killed and genuinely restarted from disk throughout.
     */
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {11, 29, 77})
    void staysLinearizableAcrossRestarts(long seed) {
        SimulatedCluster cluster = new SimulatedCluster(5, seed);
        StoreDriver driver = new StoreDriver(cluster);

        cluster.tickUntilLeaderElected(BUDGET);
        driver.tick(20);

        for (int round = 0; round < 15; round++) {
            driver.put(1, "k" + (round % 3), "v" + round);
            driver.get(2, "k" + (round % 3));
            driver.tick(15);

            // Kill and immediately restart a node, so it must recover from its store.
            String victim = "n" + (round % 5);
            cluster.crash(victim);
            driver.tick(10);
            cluster.restart(victim);
            driver.tick(25);
        }

        driver.tick(500);
        driver.abandonOutstanding();

        var result = LinearizabilityChecker.check(driver.history());
        assertThat(result.outcome())
                .as("history of %d operations: %s", driver.history().size(), result)
                .isEqualTo(LinearizabilityChecker.Outcome.LINEARIZABLE);

        long completedReads =
                driver.history().operations().stream()
                        .filter(
                                o ->
                                        o.kind()
                                                        == io.github.ankitnehra6.raftkv
                                                                .linearizability.Operation.Kind.GET
                                                && !o.isPending())
                        .count();
        assertThat(completedReads)
                .as("too few completed reads for the check to mean anything")
                .isGreaterThanOrEqualTo(5);
    }

    @Test
    void aNodeThatNeverRanStartsFromInitialState() {
        SimulatedCluster cluster = new SimulatedCluster(3, 44);

        assertThat(cluster.storeAt("n0").loadState()).isEqualTo(PersistentState.INITIAL);
        assertThat(cluster.storeAt("n0").readAll()).isEmpty();
        assertThat(cluster.node("n0").currentTerm()).isZero();
        assertThat(cluster.node("n0").votedFor()).isEmpty();
        assertThat(List.of(cluster.node("n0").role())).containsExactly(Role.FOLLOWER);
    }
}
