package io.github.ankitnehra6.raftkv.linearizability;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A record of everything clients asked for and were told.
 *
 * <p>Collected by the test driver as operations happen, then handed to
 * {@link LinearizabilityChecker} afterwards. Nothing here inspects the cluster's internals:
 * a history contains only what a client could observe from outside, which is the only
 * evidence a correctness claim should rest on.
 */
public class History {

    private final List<Operation> operations = new ArrayList<>();
    private final AtomicLong nextId = new AtomicLong();

    /** Starts recording an operation, returning a handle to complete it with. */
    public Pending invoke(Operation.Kind kind, int process, String key, String writeValue, long at) {
        return new Pending(nextId.getAndIncrement(), kind, process, key, writeValue, at);
    }

    /** An operation that has been issued but not yet answered. */
    public final class Pending {
        private final long id;
        private final Operation.Kind kind;
        private final int process;
        private final String key;
        private final String writeValue;
        private final long invokedAt;

        private Pending(
                long id,
                Operation.Kind kind,
                int process,
                String key,
                String writeValue,
                long invokedAt) {
            this.id = id;
            this.kind = kind;
            this.process = process;
            this.key = key;
            this.writeValue = writeValue;
            this.invokedAt = invokedAt;
        }

        /** Records a successful completion. {@code observed} is only meaningful for GET. */
        public void complete(String observed, long at) {
            operations.add(
                    new Operation(id, process, kind, key, writeValue, observed, invokedAt, at));
        }

        /**
         * Records that the client never got an answer.
         *
         * <p>Kept in the history rather than discarded: a request lost in a partition may
         * still have been applied, and dropping it would let the checker reject a correct
         * store for "inventing" a value it was legitimately told to write.
         */
        public void abandon() {
            operations.add(
                    new Operation(
                            id, process, kind, key, writeValue, null, invokedAt, Long.MAX_VALUE));
        }
    }

    public List<Operation> operations() {
        return List.copyOf(operations);
    }

    public int size() {
        return operations.size();
    }

    /**
     * Splits the history by key.
     *
     * <p>Operations on different keys are independent in a key-value store, so each key can
     * be checked on its own. This matters enormously: linearizability checking is
     * exponential in the number of concurrent operations, and partitioning turns one
     * intractable search into many small ones.
     */
    public Map<String, List<Operation>> byKey() {
        return operations.stream()
                .collect(
                        Collectors.groupingBy(
                                Operation::key, LinkedHashMap::new, Collectors.toList()));
    }

    @Override
    public String toString() {
        return "History[%d operations across %d keys]".formatted(operations.size(), byKey().size());
    }
}
