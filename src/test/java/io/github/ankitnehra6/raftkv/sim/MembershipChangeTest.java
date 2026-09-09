package io.github.ankitnehra6.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ankitnehra6.raftkv.core.ClusterConfig;
import io.github.ankitnehra6.raftkv.core.NodeId;
import io.github.ankitnehra6.raftkv.core.RaftNode;
import io.github.ankitnehra6.raftkv.linearizability.LinearizabilityChecker;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Adding and removing servers while the cluster is running.
 *
 * <p>Changes are made one server at a time. Arbitrary changes need joint consensus: going
 * from {a,b,c} to {c,d,e} in one step lets {a,b} and {d,e} form disjoint majorities and
 * elect two leaders. Changing by one keeps the old and new majorities overlapping, so that
 * cannot happen without the extra machinery.
 */
class MembershipChangeTest {

    private static final int BUDGET = 400;

    private static byte[] command(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static SimulatedCluster elected(int size, long seed) {
        SimulatedCluster cluster = new SimulatedCluster(size, seed);
        assertThat(cluster.tickUntilLeaderElected(BUDGET)).isTrue();
        cluster.tick(20);
        return cluster;
    }

    @Test
    void aNewServerJoinsAndParticipates() {
        SimulatedCluster cluster = elected(3, 1);
        RaftNode leader = cluster.leader().orElseThrow();

        leader.propose(command("before-join"));
        cluster.tick(40);

        cluster.provision("n9");
        assertThat(leader.addServer(NodeId.of("n9"))).isPresent();
        cluster.tick(150);

        assertThat(leader.configuration().size())
                .as("the leader must now count majorities out of four")
                .isEqualTo(4);
        assertThat(leader.configuration().contains(NodeId.of("n9"))).isTrue();

        // The new server must have been caught up and must know the configuration.
        RaftNode joined = cluster.node("n9");
        assertThat(joined.configuration().contains(NodeId.of("n9"))).isTrue();
        assertThat(joined.log().lastIndex())
                .as("a joined server must receive the existing log")
                .isEqualTo(leader.log().lastIndex());

        // And new writes must reach it.
        leader.propose(command("after-join"));
        cluster.tick(80);
        assertThat(joined.log().lastIndex()).isEqualTo(leader.log().lastIndex());
    }

    @Test
    void everyMemberLearnsTheNewConfiguration() {
        SimulatedCluster cluster = elected(3, 2);
        RaftNode leader = cluster.leader().orElseThrow();

        cluster.provision("n9");
        leader.addServer(NodeId.of("n9"));
        cluster.tick(150);

        for (RaftNode node : cluster.nodes()) {
            assertThat(node.configuration().size())
                    .as("%s is still using the old configuration", node.id())
                    .isEqualTo(4);
        }
    }

    @Test
    void aRemovedServerLeavesTheConfiguration() {
        SimulatedCluster cluster = elected(5, 3);
        RaftNode leader = cluster.leader().orElseThrow();

        NodeId victim =
                cluster.nodes().stream()
                        .map(RaftNode::id)
                        .filter(nodeId -> !nodeId.equals(leader.id()))
                        .findFirst()
                        .orElseThrow();

        assertThat(leader.removeServer(victim)).isPresent();
        cluster.tick(150);

        assertThat(leader.configuration().size()).isEqualTo(4);
        assertThat(leader.configuration().contains(victim)).isFalse();
        assertThat(leader.knownPeers()).doesNotContain(victim);
    }

    /**
     * After a removal the majority is out of the smaller cluster. A three-node cluster that
     * loses one member must still make progress with two, which it could not do if it were
     * still counting out of three.
     */
    @Test
    void majorityIsComputedFromTheNewConfiguration() {
        SimulatedCluster cluster = elected(3, 4);
        RaftNode leader = cluster.leader().orElseThrow();

        NodeId victim =
                cluster.nodes().stream()
                        .map(RaftNode::id)
                        .filter(nodeId -> !nodeId.equals(leader.id()))
                        .findFirst()
                        .orElseThrow();

        leader.removeServer(victim);
        cluster.tick(120);
        assertThat(leader.configuration().size()).isEqualTo(2);

        // Take the removed node offline entirely; the remaining two are a full cluster.
        cluster.crash(victim.value());

        long before = leader.commitIndex();
        leader.propose(command("after-removal"));
        cluster.tick(120);

        assertThat(leader.commitIndex())
                .as("two of two is a majority, so this must commit")
                .isGreaterThan(before);
    }

    /**
     * Overlapping changes are exactly what joint consensus exists to handle. Refusing the
     * second is the honest alternative to implementing it.
     */
    @Test
    void refusesASecondChangeWhileOneIsInFlight() {
        SimulatedCluster cluster = elected(3, 5);
        RaftNode leader = cluster.leader().orElseThrow();

        cluster.provision("n8");
        cluster.provision("n9");

        assertThat(leader.addServer(NodeId.of("n8"))).isPresent();
        assertThat(leader.addServer(NodeId.of("n9")))
                .as("a second change must wait for the first to commit")
                .isEmpty();

        cluster.tick(150);

        // Once the first has committed, the second is accepted.
        assertThat(leader.addServer(NodeId.of("n9"))).isPresent();
    }

    @Test
    void onlyTheLeaderChangesMembership() {
        SimulatedCluster cluster = elected(3, 6);
        RaftNode leader = cluster.leader().orElseThrow();

        RaftNode follower =
                cluster.nodes().stream().filter(n -> !n.equals(leader)).findFirst().orElseThrow();

        cluster.provision("n9");
        assertThat(follower.addServer(NodeId.of("n9"))).isEmpty();
    }

    @Test
    void refusesToRemoveTheLastMember() {
        SimulatedCluster cluster = elected(1, 7);
        RaftNode only = cluster.leader().orElseThrow();

        assertThatThrownBy(() -> only.removeServer(only.id()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("last member");
    }

    @Test
    void aNoOpChangeIsRejected() {
        SimulatedCluster cluster = elected(3, 8);
        RaftNode leader = cluster.leader().orElseThrow();

        assertThat(leader.addServer(leader.id()))
                .as("already a member")
                .isEmpty();
        assertThat(leader.removeServer(NodeId.of("never-existed")))
                .as("not a member")
                .isEmpty();
    }

    /** Membership is in the log, so it must come back after a restart. */
    @Test
    void theConfigurationSurvivesARestart() {
        SimulatedCluster cluster = elected(3, 9);
        RaftNode leader = cluster.leader().orElseThrow();

        cluster.provision("n9");
        leader.addServer(NodeId.of("n9"));
        cluster.tick(150);

        String restarted = "n0";
        assertThat(cluster.node(restarted).configuration().size()).isEqualTo(4);

        cluster.crash(restarted);
        cluster.restart(restarted);

        assertThat(cluster.node(restarted).configuration().size())
                .as("membership lives in the log and must be recovered from it")
                .isEqualTo(4);
        assertThat(cluster.node(restarted).configuration().contains(NodeId.of("n9"))).isTrue();
    }

    /** The end-to-end claim: membership changes must not make the store observably wrong. */
    @Test
    void staysLinearizableAcrossAMembershipChange() {
        SimulatedCluster cluster = elected(3, 10);
        StoreDriver driver = new StoreDriver(cluster);
        driver.tick(20);

        for (int i = 0; i < 8; i++) {
            driver.put(1, "k", "v" + i);
            driver.get(2, "k");
            driver.tick(12);
        }

        cluster.provision("n9");
        cluster.leader().orElseThrow().addServer(NodeId.of("n9"));
        driver.tick(120);

        for (int i = 8; i < 16; i++) {
            driver.put(1, "k", "v" + i);
            driver.get(2, "k");
            driver.tick(12);
        }

        driver.tick(300);
        driver.abandonOutstanding();

        var result = LinearizabilityChecker.check(driver.history());
        assertThat(result.outcome())
                .as("history of %d operations: %s", driver.history().size(), result)
                .isEqualTo(LinearizabilityChecker.Outcome.LINEARIZABLE);
    }

    /**
     * §4.2.3: a server outside the configuration must not be able to disrupt it.
     *
     * <p>A provisioned-but-unadded server never hears from the leader, so it times out and
     * campaigns with an ever-higher term. Without the guard, the term rule forces the real
     * leader to step down on every attempt and the cluster churns through elections
     * indefinitely while making no progress. This was a genuine bug, found because a
     * membership test failed for a reason that had nothing to do with membership.
     */
    @Test
    void anOutsiderCannotDisruptAHealthyCluster() {
        SimulatedCluster cluster = elected(3, 5);
        RaftNode leader = cluster.leader().orElseThrow();
        long termBefore = leader.currentTerm();

        // Bring up a server and never add it. It will campaign forever.
        cluster.provision("n9");
        cluster.tick(400);

        assertThat(cluster.leader())
                .as("the original leader must still be leading")
                .map(RaftNode::id)
                .contains(leader.id());
        assertThat(leader.currentTerm())
                .as("an outsider must not be able to drive the term up")
                .isEqualTo(termBefore);

        // And the cluster must still be able to commit.
        long commitBefore = leader.commitIndex();
        leader.propose(command("still-working"));
        cluster.tick(80);
        assertThat(leader.commitIndex()).isGreaterThan(commitBefore);
    }

    /** The guard must not block a legitimate election when the leader really is gone. */
    @Test
    void theGuardStillAllowsARealElection() {
        SimulatedCluster cluster = elected(5, 11);
        RaftNode original = cluster.leader().orElseThrow();

        cluster.crash(original.id().value());

        assertThat(cluster.tickUntil(BUDGET, () -> cluster.leader().isPresent()))
                .as("heartbeats stopped, so the guard must expire and an election proceed")
                .isTrue();
        assertThat(cluster.leader().orElseThrow().id()).isNotEqualTo(original.id());
    }

    // --- encoding --------------------------------------------------------------------

    @Test
    void configurationsRoundTrip() {
        ClusterConfig original = ClusterConfig.of(NodeId.of("a"), NodeId.of("b"), NodeId.of("c"));

        assertThat(ClusterConfig.decode(original.encode())).isEqualTo(original);
    }

    /** Identical memberships must encode identically, whatever order they were built in. */
    @Test
    void encodingIsDeterministic() {
        ClusterConfig one = ClusterConfig.of(NodeId.of("c"), NodeId.of("a"), NodeId.of("b"));
        ClusterConfig other = ClusterConfig.of(NodeId.of("a"), NodeId.of("b"), NodeId.of("c"));

        assertThat(one.encode()).isEqualTo(other.encode());
    }

    @Test
    void majorityMathIsCorrect() {
        assertThat(ClusterConfig.of(NodeId.of("a")).isMajority(1)).isTrue();

        ClusterConfig three = ClusterConfig.of(NodeId.of("a"), NodeId.of("b"), NodeId.of("c"));
        assertThat(three.isMajority(1)).isFalse();
        assertThat(three.isMajority(2)).isTrue();

        ClusterConfig four =
                ClusterConfig.of(NodeId.of("a"), NodeId.of("b"), NodeId.of("c"), NodeId.of("d"));
        assertThat(four.isMajority(2)).as("half is not a majority").isFalse();
        assertThat(four.isMajority(3)).isTrue();
    }

    @Test
    void rejectsAnEmptyConfiguration() {
        assertThatThrownBy(() -> ClusterConfig.decode(new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
