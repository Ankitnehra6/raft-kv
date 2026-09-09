package io.github.ankitnehra6.raftkv.linearizability;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for the checker itself.
 *
 * <p>These matter more than they look. A linearizability checker that always answers "yes"
 * would make every other test in this project pass while proving nothing, so the checker
 * has to be shown to <em>reject</em> histories that are genuinely impossible — not merely
 * to accept ones that are fine.
 *
 * <p>Times are plain integers standing for ticks. Two operations overlap when their
 * intervals overlap.
 */
class LinearizabilityCheckerTest {

    private static final long BUDGET = 1_000_000L;

    private static LinearizabilityChecker.Result check(Operation... ops) {
        return LinearizabilityChecker.checkKey("k", List.of(ops), BUDGET);
    }

    // --- histories that must be accepted -----------------------------------------

    @Test
    void acceptsASimpleSequentialHistory() {
        var result =
                check(
                        Operation.put(0, 1, "k", "a", 0, 10),
                        Operation.get(1, 1, "k", "a", 20, 30));

        assertThat(result.isLinearizable()).as("%s", result).isTrue();
    }

    @Test
    void acceptsAReadOfAnAbsentKey() {
        var result = check(Operation.get(0, 1, "k", null, 0, 10));

        assertThat(result.isLinearizable()).as("%s", result).isTrue();
    }

    @Test
    void acceptsADeleteFollowedByAnAbsentRead() {
        var result =
                check(
                        Operation.put(0, 1, "k", "a", 0, 10),
                        Operation.delete(1, 1, "k", 20, 30),
                        Operation.get(2, 1, "k", null, 40, 50));

        assertThat(result.isLinearizable()).as("%s", result).isTrue();
    }

    /**
     * Two writes overlap, so either could have won. A read seeing either is explainable —
     * this is the freedom linearizability grants, and a checker that rejected it would be
     * too strict to be usable.
     */
    @Test
    void acceptsEitherWinnerOfConcurrentWrites() {
        var seesA =
                check(
                        Operation.put(0, 1, "k", "a", 0, 20),
                        Operation.put(1, 2, "k", "b", 5, 25),
                        Operation.get(2, 3, "k", "a", 30, 40));

        var seesB =
                check(
                        Operation.put(0, 1, "k", "a", 0, 20),
                        Operation.put(1, 2, "k", "b", 5, 25),
                        Operation.get(2, 3, "k", "b", 30, 40));

        assertThat(seesA.isLinearizable()).as("%s", seesA).isTrue();
        assertThat(seesB.isLinearizable()).as("%s", seesB).isTrue();
    }

    /**
     * A read overlapping a write may see the old value or the new one, but once a later,
     * non-overlapping read sees the new value the ordering is pinned.
     */
    @Test
    void acceptsAReadThatOverlapsAWrite() {
        var result =
                check(
                        Operation.put(0, 1, "k", "old", 0, 10),
                        Operation.put(1, 1, "k", "new", 20, 40),
                        Operation.get(2, 2, "k", "old", 25, 35),
                        Operation.get(3, 2, "k", "new", 50, 60));

        assertThat(result.isLinearizable()).as("%s", result).isTrue();
    }

    /**
     * A write whose response was lost may or may not have been applied, so a later read
     * seeing the older value is legitimate. Dropping pending writes from the history would
     * make this a false violation.
     */
    @Test
    void acceptsAReadThatIgnoresAPendingWrite() {
        var result =
                check(
                        Operation.put(0, 1, "k", "committed", 0, 10),
                        Operation.pending(Operation.Kind.PUT, 1, 2, "k", "maybe", 20),
                        Operation.get(2, 3, "k", "committed", 30, 40));

        assertThat(result.isLinearizable()).as("%s", result).isTrue();
    }

    /** And equally, a read that *does* see the pending write is fine: it may have landed. */
    @Test
    void acceptsAReadThatSeesAPendingWrite() {
        var result =
                check(
                        Operation.put(0, 1, "k", "committed", 0, 10),
                        Operation.pending(Operation.Kind.PUT, 1, 2, "k", "maybe", 20),
                        Operation.get(2, 3, "k", "maybe", 30, 40));

        assertThat(result.isLinearizable()).as("%s", result).isTrue();
    }

    // --- histories that must be rejected ------------------------------------------

    /** Nothing ever wrote "ghost", so no ordering can produce it. */
    @Test
    void rejectsAValueThatWasNeverWritten() {
        var result =
                check(
                        Operation.put(0, 1, "k", "a", 0, 10),
                        Operation.get(1, 1, "k", "ghost", 20, 30));

        assertThat(result.outcome())
                .as("%s", result)
                .isEqualTo(LinearizabilityChecker.Outcome.NOT_LINEARIZABLE);
    }

    /**
     * The canonical stale read. The write completed at t=10 and the read started at t=20,
     * so real-time order forces the write first — yet the read saw the old value. This is
     * exactly the bug a leader serving reads from a stale log would produce.
     */
    @Test
    void rejectsAStaleReadAfterACompletedWrite() {
        var result =
                check(
                        Operation.put(0, 1, "k", "old", 0, 5),
                        Operation.put(1, 1, "k", "new", 6, 10),
                        Operation.get(2, 2, "k", "old", 20, 30));

        assertThat(result.outcome())
                .as("%s", result)
                .isEqualTo(LinearizabilityChecker.Outcome.NOT_LINEARIZABLE);
    }

    /**
     * Time going backwards: a later read sees an earlier value than one before it, with no
     * intervening write. This is what a split-brain looks like from a client's side.
     */
    @Test
    void rejectsReadsThatGoBackwardsInTime() {
        var result =
                check(
                        Operation.put(0, 1, "k", "v1", 0, 5),
                        Operation.put(1, 1, "k", "v2", 10, 15),
                        Operation.get(2, 2, "k", "v2", 20, 25),
                        Operation.get(3, 2, "k", "v1", 30, 35));

        assertThat(result.outcome())
                .as("%s", result)
                .isEqualTo(LinearizabilityChecker.Outcome.NOT_LINEARIZABLE);
    }

    @Test
    void rejectsAReadOfADeletedKey() {
        var result =
                check(
                        Operation.put(0, 1, "k", "a", 0, 5),
                        Operation.delete(1, 1, "k", 10, 15),
                        Operation.get(2, 1, "k", "a", 20, 25));

        assertThat(result.outcome())
                .as("%s", result)
                .isEqualTo(LinearizabilityChecker.Outcome.NOT_LINEARIZABLE);
    }

    /** Two clients reading concurrently after a settled write must agree. */
    @Test
    void rejectsTwoClientsDisagreeingAboutASettledValue() {
        var result =
                check(
                        Operation.put(0, 1, "k", "a", 0, 5),
                        Operation.get(1, 2, "k", "a", 10, 15),
                        Operation.get(2, 3, "k", "b", 20, 25));

        assertThat(result.outcome())
                .as("%s", result)
                .isEqualTo(LinearizabilityChecker.Outcome.NOT_LINEARIZABLE);
    }

    // --- mechanics -----------------------------------------------------------------

    /** Different keys are independent and must not constrain each other. */
    @Test
    void checksEachKeyIndependently() {
        History history = new History();

        var putX = history.invoke(Operation.Kind.PUT, 1, "x", "1", 0);
        putX.complete(null, 10);
        var putY = history.invoke(Operation.Kind.PUT, 1, "y", "2", 0);
        putY.complete(null, 10);
        var getX = history.invoke(Operation.Kind.GET, 1, "x", null, 20);
        getX.complete("1", 30);
        var getY = history.invoke(Operation.Kind.GET, 1, "y", null, 20);
        getY.complete("2", 30);

        var result = LinearizabilityChecker.check(history);

        assertThat(result.isLinearizable()).as("%s", result).isTrue();
        assertThat(history.byKey()).containsOnlyKeys("x", "y");
    }

    @Test
    void reportsWhichKeyFailed() {
        History history = new History();

        var good = history.invoke(Operation.Kind.PUT, 1, "fine", "v", 0);
        good.complete(null, 5);
        var goodRead = history.invoke(Operation.Kind.GET, 1, "fine", null, 10);
        goodRead.complete("v", 15);

        var write = history.invoke(Operation.Kind.PUT, 1, "broken", "v", 0);
        write.complete(null, 5);
        var badRead = history.invoke(Operation.Kind.GET, 1, "broken", null, 10);
        badRead.complete("impossible", 15);

        var result = LinearizabilityChecker.check(history);

        assertThat(result.outcome()).isEqualTo(LinearizabilityChecker.Outcome.NOT_LINEARIZABLE);
        assertThat(result.key()).isEqualTo("broken");
        assertThat(result.explanation()).contains("impossible");
    }

    /**
     * Exceeding the budget must report UNKNOWN, never success. A checker that gives up and
     * says "fine" converts an unproven claim into a false one.
     */
    @Test
    void reportsUnknownRatherThanSuccessWhenTheBudgetRunsOut() {
        // Many mutually overlapping writes plus a read: a large search space, and a budget
        // of one step cannot possibly finish it.
        Operation[] ops = new Operation[12];
        for (int i = 0; i < 11; i++) {
            ops[i] = Operation.put(i, i, "k", "v" + i, 0, 1000);
        }
        ops[11] = Operation.get(11, 99, "k", "v5", 0, 1000);

        var result = LinearizabilityChecker.checkKey("k", List.of(ops), 1);

        assertThat(result.outcome())
                .as("a budget of one step cannot prove anything")
                .isEqualTo(LinearizabilityChecker.Outcome.UNKNOWN);
        assertThat(result.isLinearizable()).isFalse();
    }

    @Test
    void handlesAnEmptyHistory() {
        assertThat(LinearizabilityChecker.check(new History()).isLinearizable()).isTrue();
    }

    /** Highly concurrent but genuinely valid histories must still be accepted. */
    @Test
    void acceptsAWideConcurrentHistory() {
        Operation[] ops = new Operation[9];
        for (int i = 0; i < 8; i++) {
            ops[i] = Operation.put(i, i, "k", "v" + i, 0, 100);
        }
        ops[8] = Operation.get(8, 99, "k", "v3", 0, 100);

        var result = LinearizabilityChecker.checkKey("k", List.of(ops), BUDGET);

        assertThat(result.isLinearizable()).as("%s", result).isTrue();
    }
}
