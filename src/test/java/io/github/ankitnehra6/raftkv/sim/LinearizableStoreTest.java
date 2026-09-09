package io.github.ankitnehra6.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ankitnehra6.raftkv.linearizability.LinearizabilityChecker;
import io.github.ankitnehra6.raftkv.linearizability.Operation;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The strongest claim this project makes: under partitions, packet loss, reordering and
 * crashes, no client ever observes anything a correct single-threaded store could not have
 * produced.
 *
 * <p>This is a different kind of test from the ones asserting on logs and terms. Those check
 * that the implementation matches the paper. This one ignores the implementation entirely
 * and asks only whether the observable behaviour is correct — which is the question a user
 * of the store actually has.
 */
class LinearizableStoreTest {

    private static final RandomGeneratorFactory<RandomGenerator> RANDOM =
            RandomGeneratorFactory.of("L64X128MixRandom");

    /** Small key space on purpose: contention on the same key is where bugs surface. */
    private static final String[] KEYS = {"a", "b", "c"};

    private static void assertLinearizable(StoreDriver driver) {
        var result = LinearizabilityChecker.check(driver.history());

        assertThat(result.outcome())
                .as(
                        "history of %d operations was not proven linearizable:%n%s",
                        driver.history().size(), result)
                .isEqualTo(LinearizabilityChecker.Outcome.LINEARIZABLE);
    }

    @Test
    void aQuietClusterIsLinearizable() {
        SimulatedCluster cluster = new SimulatedCluster(3, 1);
        StoreDriver driver = new StoreDriver(cluster);
        cluster.tickUntilLeaderElected(200);
        driver.tick(20);

        driver.put(1, "a", "1");
        driver.tick(20);
        driver.get(1, "a");
        driver.tick(20);
        driver.put(1, "a", "2");
        driver.tick(20);
        driver.get(1, "a");
        driver.tick(40);

        driver.abandonOutstanding();

        assertThat(driver.history().size()).isEqualTo(4);
        assertLinearizable(driver);
    }

    /**
     * Concurrent clients on a healthy cluster. Requests overlap, so many orderings are
     * possible; the checker verifies that at least one explains every answer.
     */
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 7, 23, 91})
    void concurrentClientsAreLinearizable(long seed) {
        SimulatedCluster cluster = new SimulatedCluster(5, seed);
        StoreDriver driver = new StoreDriver(cluster);
        RandomGenerator random = RANDOM.create(seed);

        cluster.tickUntilLeaderElected(300);
        driver.tick(20);

        for (int round = 0; round < 60; round++) {
            for (int process = 1; process <= 3; process++) {
                issueRandomOperation(driver, random, process);
            }
            driver.tick(5);
        }

        driver.tick(300);
        driver.abandonOutstanding();

        assertMeaningful(driver, 100, 40);
        assertLinearizable(driver);
    }

    /**
     * The real test. Leaders are killed, partitions open and close, and messages are lost
     * and reordered throughout — and the observable history must still be explainable.
     *
     * <p>A split-brain that let two leaders both accept writes, or a leader that served a
     * read from a stale state machine, would produce a history no ordering can explain, and
     * the checker would say so.
     */
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {3, 17, 42, 128, 999})
    void staysLinearizableUnderPartitionsAndCrashes(long seed) {
        SimulatedCluster cluster = new SimulatedCluster(5, seed);
        cluster.setDropRate(0.05);
        cluster.setDelay(1, 4);

        StoreDriver driver = new StoreDriver(cluster);
        RandomGenerator random = RANDOM.create(seed);

        cluster.tickUntilLeaderElected(400);
        driver.tick(20);

        for (int round = 0; round < 25; round++) {
            for (int process = 1; process <= 3; process++) {
                issueRandomOperation(driver, random, process);
            }
            driver.tick(8);

            switch (round % 5) {
                case 0 -> {
                    // Isolate a random minority.
                    int a = random.nextInt(5);
                    int b = (a + 1) % 5;
                    cluster.partition(
                            java.util.List.of("n" + a, "n" + b),
                            java.util.List.of(
                                    "n" + (a + 2) % 5, "n" + (a + 3) % 5, "n" + (a + 4) % 5));
                }
                case 2 -> cluster.leader().ifPresent(l -> cluster.crash(l.id().value()));
                case 3 -> {
                    cluster.heal();
                    for (int i = 0; i < 5; i++) {
                        cluster.restart("n" + i);
                    }
                }
                default -> {
                    // Let it run.
                }
            }
            driver.tick(20);
        }

        // Settle: heal everything and let the cluster finish whatever it can.
        cluster.heal();
        for (int i = 0; i < 5; i++) {
            cluster.restart("n" + i);
        }
        driver.tick(600);
        driver.abandonOutstanding();

        // Guards against passing vacuously. A run that produced almost nothing, or whose
        // reads all failed to complete, would satisfy the checker while proving nothing —
        // reads are where a stale answer would show up, so there have to be plenty.
        assertMeaningful(driver, 25, 10);
        assertLinearizable(driver);
    }

    /**
     * A minority partition must not be able to serve reads either. If a stranded leader
     * answered from its own state machine, a client on that side would see a value the
     * majority had already replaced — a history no ordering can explain.
     */
    @Test
    void aStrandedLeaderCannotServeStaleReads() {
        SimulatedCluster cluster = new SimulatedCluster(5, 64);
        StoreDriver driver = new StoreDriver(cluster);

        cluster.tickUntilLeaderElected(300);
        driver.tick(20);

        driver.put(1, "a", "before");
        driver.tick(40);

        var stranded = cluster.leader().orElseThrow();
        var majority =
                cluster.nodes().stream()
                        .map(n -> n.id().value())
                        .filter(name -> !name.equals(stranded.id().value()))
                        .toList();
        cluster.partition(java.util.List.of(stranded.id().value()), majority);

        // A client on the stranded side keeps trying to read; none of these may succeed
        // with a stale value.
        for (int i = 0; i < 20; i++) {
            driver.get(2, "a");
            driver.tick(10);
        }

        // Meanwhile the majority elects a new leader and moves on.
        cluster.tickUntil(400, () -> majority.stream().map(cluster::node).anyMatch(n -> n.isLeader()));
        driver.tick(50);
        driver.put(1, "a", "after");
        driver.tick(80);

        cluster.heal();
        driver.tick(400);
        driver.abandonOutstanding();

        assertLinearizable(driver);
    }

    /** Fails if the run was too small or too read-poor for the check to mean anything. */
    private static void assertMeaningful(StoreDriver driver, int minOperations, int minReads) {
        var ops = driver.history().operations();
        long completedReads =
                ops.stream()
                        .filter(o -> o.kind() == Operation.Kind.GET && !o.isPending())
                        .count();

        assertThat(ops.size())
                .as("history too small to prove anything")
                .isGreaterThanOrEqualTo(minOperations);
        assertThat(completedReads)
                .as("too few completed reads; a stale answer would have nowhere to show up")
                .isGreaterThanOrEqualTo(minReads);
    }

    private void issueRandomOperation(StoreDriver driver, RandomGenerator random, int process) {
        String key = KEYS[random.nextInt(KEYS.length)];
        int roll = random.nextInt(10);

        if (roll < 5) {
            driver.get(process, key);
        } else if (roll < 9) {
            driver.put(process, key, "v" + random.nextInt(1000));
        } else {
            driver.delete(process, key);
        }
    }
}
