package io.github.ankitnehra6.raftkv.linearizability;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Decides whether a recorded history could have come from a correct key-value store.
 *
 * <p>Linearizability is the guarantee Raft is supposed to deliver: every operation appears
 * to take effect instantaneously at some point between when the client sent it and when the
 * client got an answer, and those points are consistent with a single sequential execution.
 * Checking it turns "the tests pass" into "no client could have observed anything a correct
 * store would not produce", which is a much stronger claim and the only one worth making
 * about a consensus implementation.
 *
 * <p>The search is the Wing &amp; Gong algorithm with the refinements Lowe describes: try to
 * linearize a minimal operation, recurse, and backtrack when stuck. Two things keep it
 * tractable:
 *
 * <ul>
 *   <li><b>Partition by key.</b> Operations on different keys are independent, so one
 *       intractable search becomes many small ones. This is the difference between seconds
 *       and never finishing.
 *   <li><b>Memoise on (linearized set, state).</b> The same subproblem is reached by many
 *       different orders, and without this the search re-explores each of them.
 * </ul>
 *
 * <p>A step budget bounds the work. Exceeding it reports {@link Outcome#UNKNOWN} rather
 * than success — a checker that gives up and says "fine" is worse than no checker, because
 * it converts an unproven claim into a false one.
 */
public final class LinearizabilityChecker {

    /** Default ceiling on search steps, per key. Generous for test-sized histories. */
    public static final long DEFAULT_STEP_BUDGET = 2_000_000L;

    public enum Outcome {
        /** A valid sequential ordering exists. */
        LINEARIZABLE,
        /** No ordering can explain the observed results. The store is wrong. */
        NOT_LINEARIZABLE,
        /** The search exceeded its budget. Nothing is proven either way. */
        UNKNOWN
    }

    /**
     * @param outcome the verdict
     * @param key the key whose sub-history produced it, if any
     * @param explanation human-readable detail, for a test failure message
     */
    public record Result(Outcome outcome, String key, String explanation) {

        public boolean isLinearizable() {
            return outcome == Outcome.LINEARIZABLE;
        }

        @Override
        public String toString() {
            return key == null ? "%s: %s".formatted(outcome, explanation)
                    : "%s on key '%s': %s".formatted(outcome, key, explanation);
        }
    }

    private LinearizabilityChecker() {}

    /** Checks a whole history, key by key. */
    public static Result check(History history) {
        return check(history, DEFAULT_STEP_BUDGET);
    }

    public static Result check(History history, long stepBudget) {
        for (Map.Entry<String, List<Operation>> entry : history.byKey().entrySet()) {
            Result result = checkKey(entry.getKey(), entry.getValue(), stepBudget);
            if (result.outcome() != Outcome.LINEARIZABLE) {
                return result;
            }
        }
        return new Result(
                Outcome.LINEARIZABLE,
                null,
                "all %d operations across %d keys are explained by a sequential ordering"
                        .formatted(history.size(), history.byKey().size()));
    }

    /** Checks the sub-history for one key. */
    public static Result checkKey(String key, List<Operation> operations, long stepBudget) {
        // A pending read carries no information: the client never learned what it saw, so
        // there is nothing to contradict. Pending *writes* are kept, because they may or
        // may not have been applied and the checker must be free to place them anywhere.
        List<Operation> relevant =
                operations.stream()
                        .filter(op -> !(op.kind() == Operation.Kind.GET && op.isPending()))
                        .sorted(
                                Comparator.comparingLong(Operation::invokedAt)
                                        .thenComparingLong(Operation::id))
                        .toList();

        if (relevant.isEmpty()) {
            return new Result(Outcome.LINEARIZABLE, key, "nothing to check");
        }

        Search search = new Search(relevant, stepBudget);
        return switch (search.run()) {
            case LINEARIZABLE ->
                    new Result(
                            Outcome.LINEARIZABLE,
                            key,
                            "%d operations linearized in %d steps"
                                    .formatted(relevant.size(), search.steps));
            case NOT_LINEARIZABLE ->
                    new Result(
                            Outcome.NOT_LINEARIZABLE,
                            key,
                            "no sequential ordering explains these %d operations:%n%s"
                                    .formatted(relevant.size(), render(relevant)));
            case UNKNOWN ->
                    new Result(
                            Outcome.UNKNOWN,
                            key,
                            "search exceeded %d steps over %d operations; nothing is proven"
                                    .formatted(stepBudget, relevant.size()));
        };
    }

    private static String render(List<Operation> operations) {
        StringBuilder sb = new StringBuilder();
        for (Operation op : operations) {
            sb.append("  ").append(op).append(System.lineSeparator());
        }
        return sb.toString();
    }

    /** The backtracking search over one key's sub-history. */
    private static final class Search {

        private final List<Operation> ops;
        private final long budget;
        private final Set<MemoKey> seen = new HashSet<>();

        private long steps;

        Search(List<Operation> ops, long budget) {
            this.ops = ops;
            this.budget = budget;
        }

        /** Memoisation key: which operations are already placed, and the resulting value. */
        private record MemoKey(BitSet linearized, String state) {}

        Outcome run() {
            BitSet linearized = new BitSet(ops.size());
            return recurse(linearized, ops.size(), null);
        }

        /**
         * @param linearized which operations have been placed
         * @param remaining how many are left
         * @param state the register's value after the placed prefix; null means absent
         */
        private Outcome recurse(BitSet linearized, int remaining, String state) {
            if (remaining == 0) {
                return Outcome.LINEARIZABLE;
            }
            if (++steps > budget) {
                return Outcome.UNKNOWN;
            }

            MemoKey memo = new MemoKey((BitSet) linearized.clone(), state);
            if (!seen.add(memo)) {
                // This exact subproblem already failed; re-exploring it cannot help.
                return Outcome.NOT_LINEARIZABLE;
            }

            boolean sawUnknown = false;

            for (int i : candidates(linearized)) {
                Operation op = ops.get(i);

                String next;
                switch (op.kind()) {
                    case GET -> {
                        if (!Objects.equals(op.readValue(), state)) {
                            continue; // this read cannot have happened here
                        }
                        next = state;
                    }
                    case PUT -> next = op.writeValue();
                    case DELETE -> next = null;
                    default -> throw new IllegalStateException("unhandled kind " + op.kind());
                }

                linearized.set(i);
                Outcome outcome = recurse(linearized, remaining - 1, next);
                linearized.clear(i);

                if (outcome == Outcome.LINEARIZABLE) {
                    return Outcome.LINEARIZABLE;
                }
                if (outcome == Outcome.UNKNOWN) {
                    sawUnknown = true;
                }
            }

            return sawUnknown ? Outcome.UNKNOWN : Outcome.NOT_LINEARIZABLE;
        }

        /**
         * Operations that could legally come next.
         *
         * <p>An operation may be placed next only if no other unplaced operation is
         * required to precede it — that is, none finished before this one started. That
         * real-time constraint is what makes this linearizability rather than mere
         * sequential consistency.
         */
        private List<Integer> candidates(BitSet linearized) {
            List<Integer> unplaced = new ArrayList<>();
            for (int i = 0; i < ops.size(); i++) {
                if (!linearized.get(i)) {
                    unplaced.add(i);
                }
            }

            List<Integer> result = new ArrayList<>(unplaced.size());
            for (int i : unplaced) {
                long invokedAt = ops.get(i).invokedAt();
                boolean blocked = false;

                for (int j : unplaced) {
                    if (i != j && ops.get(j).completedAt() < invokedAt) {
                        blocked = true;
                        break;
                    }
                }
                if (!blocked) {
                    result.add(i);
                }
            }
            return result;
        }
    }
}
