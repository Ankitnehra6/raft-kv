package io.github.ankitnehra6.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ankitnehra6.raftkv.core.RaftNode;
import io.github.ankitnehra6.raftkv.linearizability.LinearizabilityChecker;
import io.github.ankitnehra6.raftkv.statemachine.Command;
import io.github.ankitnehra6.raftkv.statemachine.KeyValueStore;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Snapshotting and log compaction.
 *
 * <p>Without these the log grows forever: a cluster up for a year keeps every write ever
 * made, a restarting node replays all of them, and a follower away for an hour is sent an
 * hour of history. The interesting cases are not "a snapshot was taken" but "a follower
 * needed entries that no longer exist".
 */
class SnapshotTest {

    private static final int BUDGET = 400;

    private static SimulatedCluster elected(int size, long seed) {
        SimulatedCluster cluster = new SimulatedCluster(size, seed);
        assertThat(cluster.tickUntilLeaderElected(BUDGET)).isTrue();
        return cluster;
    }

    @Test
    void compactionDiscardsTheLogPrefix() {
        SimulatedCluster cluster = elected(3, 1);
        StoreDriver driver = new StoreDriver(cluster);
        driver.tick(20);

        for (int i = 0; i < 20; i++) {
            driver.put(1, "k" + i, "v" + i);
            driver.tick(6);
        }
        driver.tick(60);

        RaftNode leader = cluster.leader().orElseThrow();
        long sizeBefore = leader.log().size();
        assertThat(sizeBefore).isGreaterThan(15);

        driver.compactAll();

        assertThat(leader.log().size())
                .as("compaction must actually shrink the log")
                .isLessThan((int) sizeBefore);
        assertThat(leader.log().snapshotIndex()).isPositive();
        assertThat(leader.snapshot()).isPresent();
    }

    /** The boundary the snapshot covers must still answer consistency checks. */
    @Test
    void theSnapshotBoundaryStillMatches() {
        SimulatedCluster cluster = elected(3, 2);
        StoreDriver driver = new StoreDriver(cluster);
        driver.tick(20);

        for (int i = 0; i < 10; i++) {
            driver.put(1, "k", "v" + i);
            driver.tick(6);
        }
        driver.tick(60);
        driver.compactAll();

        RaftNode leader = cluster.leader().orElseThrow();
        long boundary = leader.log().snapshotIndex();

        assertThat(leader.log().entryAt(boundary))
                .as("the entry itself is gone")
                .isEmpty();
        assertThat(leader.log().termAt(boundary))
                .as("but its term must still be answerable, or replication stalls here")
                .isEqualTo(leader.log().snapshotTerm());
        assertThat(leader.log().matches(boundary, leader.log().snapshotTerm())).isTrue();
    }

    /**
     * The case snapshots exist for: a follower away long enough that the entries it needs
     * have been compacted away must be caught up with state instead.
     */
    @Test
    void aFollowerTooFarBehindReceivesASnapshot() {
        SimulatedCluster cluster = elected(3, 3);
        StoreDriver driver = new StoreDriver(cluster);
        driver.tick(20);

        String laggard =
                cluster.nodes().stream()
                        .map(n -> n.id().value())
                        .filter(name -> !name.equals(cluster.leader().orElseThrow().id().value()))
                        .findFirst()
                        .orElseThrow();

        cluster.crash(laggard);

        for (int i = 0; i < 25; i++) {
            driver.put(1, "key" + i, "value" + i);
            driver.tick(6);
        }
        driver.tick(60);

        // Compact past everything the absent follower is missing.
        driver.compactAll();
        RaftNode leader = cluster.leader().orElseThrow();
        assertThat(leader.log().snapshotIndex()).isGreaterThan(10);

        cluster.restart(laggard);
        driver.tick(200);

        RaftNode recovered = cluster.node(laggard);
        assertThat(recovered.snapshot())
                .as("the follower could not be caught up with entries, so it must have "
                        + "received a snapshot")
                .isPresent();

        assertThat(driver.storeAt(recovered.id()).snapshot())
                .as("its state machine must match the leader's")
                .isEqualTo(driver.storeAt(leader.id()).snapshot());
    }

    @Test
    void aSnapshotSurvivesARestart() {
        SimulatedCluster cluster = elected(3, 4);
        StoreDriver driver = new StoreDriver(cluster);
        driver.tick(20);

        for (int i = 0; i < 15; i++) {
            driver.put(1, "k" + i, "v" + i);
            driver.tick(6);
        }
        driver.tick(60);
        driver.compactAll();

        RaftNode before = cluster.node("n0");
        long snapshotIndex = before.log().snapshotIndex();
        assertThat(snapshotIndex).isPositive();

        cluster.crash("n0");
        cluster.restart("n0");

        RaftNode after = cluster.node("n0");
        assertThat(after.log().snapshotIndex())
                .as("a recovered node must not have to replay a compacted prefix")
                .isEqualTo(snapshotIndex);
        assertThat(after.snapshot()).isPresent();
        assertThat(after.commitIndex())
                .as("everything the snapshot covers is already committed")
                .isGreaterThanOrEqualTo(snapshotIndex);
    }

    /** Compaction must never run ahead of what has actually been applied. */
    @Test
    void refusesToCompactPastWhatHasBeenApplied() {
        SimulatedCluster cluster = elected(3, 5);
        StoreDriver driver = new StoreDriver(cluster);
        driver.tick(20);

        RaftNode leader = cluster.leader().orElseThrow();

        assertThatThrownBy(() -> leader.compact(leader.lastApplied() + 50, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only");
    }

    @Test
    void compactingTwiceToTheSamePointIsHarmless() {
        SimulatedCluster cluster = elected(3, 6);
        StoreDriver driver = new StoreDriver(cluster);
        driver.tick(20);

        for (int i = 0; i < 10; i++) {
            driver.put(1, "k" + i, "v" + i);
            driver.tick(6);
        }
        driver.tick(60);

        driver.compactAll();
        long first = cluster.node("n0").log().snapshotIndex();
        driver.compactAll();

        assertThat(cluster.node("n0").log().snapshotIndex()).isEqualTo(first);
    }

    // --- state machine serialisation ---------------------------------------------

    @Test
    void stateMachineSnapshotsRoundTrip() {
        KeyValueStore original = new KeyValueStore();
        original.apply(entry(1, new Command.Put("a", "1")));
        original.apply(entry(2, new Command.Put("b", "2")));
        original.apply(entry(3, new Command.Delete("a")));

        KeyValueStore restored = new KeyValueStore();
        restored.restore(original.snapshotBytes(), 3);

        assertThat(restored.snapshot()).isEqualTo(Map.of("b", "2"));
        assertThat(restored.lastAppliedIndex()).isEqualTo(3);
    }

    /**
     * A snapshot is the complete state at its index, so restoring must clear whatever was
     * there — otherwise a key the leader deleted while this node was away would survive.
     */
    @Test
    void restoringReplacesRatherThanMerges() {
        KeyValueStore source = new KeyValueStore();
        source.apply(entry(1, new Command.Put("kept", "yes")));

        KeyValueStore target = new KeyValueStore();
        target.apply(entry(1, new Command.Put("stale", "should-be-gone")));

        target.restore(source.snapshotBytes(), 1);

        assertThat(target.snapshot()).containsOnlyKeys("kept");
    }

    @Test
    void handlesAnEmptyAndUnicodeState() {
        KeyValueStore empty = new KeyValueStore();
        KeyValueStore restored = new KeyValueStore();
        restored.restore(empty.snapshotBytes(), 0);
        assertThat(restored.size()).isZero();

        KeyValueStore unicode = new KeyValueStore();
        unicode.apply(entry(1, new Command.Put("नाम", "अंकित")));
        KeyValueStore back = new KeyValueStore();
        back.restore(unicode.snapshotBytes(), 1);
        assertThat(back.get("नाम")).contains("अंकित");
    }

    @Test
    void rejectsAMalformedSnapshot() {
        KeyValueStore store = new KeyValueStore();

        assertThatThrownBy(() -> store.restore(new byte[] {(byte) 0xFF, 0, 0, 0}, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** The end-to-end claim: compaction must not make the store observably wrong. */
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {8, 16, 32})
    void staysLinearizableWhileCompacting(long seed) {
        SimulatedCluster cluster = elected(5, seed);
        StoreDriver driver = new StoreDriver(cluster);
        driver.tick(20);

        for (int round = 0; round < 20; round++) {
            driver.put(1, "k" + (round % 3), "v" + round);
            driver.get(2, "k" + (round % 3));
            driver.tick(12);

            // Compact aggressively, and crash a node so it has to be caught up afterwards.
            driver.compactAll();
            if (round % 4 == 0) {
                cluster.crash("n" + (round % 5));
                driver.tick(15);
                cluster.restart("n" + (round % 5));
            }
            driver.tick(20);
        }

        driver.tick(400);
        driver.abandonOutstanding();

        var result = LinearizabilityChecker.check(driver.history());
        assertThat(result.outcome())
                .as("history of %d operations: %s", driver.history().size(), result)
                .isEqualTo(LinearizabilityChecker.Outcome.LINEARIZABLE);
    }

    private static io.github.ankitnehra6.raftkv.core.LogEntry entry(long index, Command command) {
        return new io.github.ankitnehra6.raftkv.core.LogEntry(1, index, Command.encode(command));
    }
}
