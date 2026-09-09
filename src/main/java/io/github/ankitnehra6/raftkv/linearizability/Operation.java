package io.github.ankitnehra6.raftkv.linearizability;

/**
 * One client operation, recorded as an interval rather than an instant.
 *
 * <p>The interval is the whole point. A client knows when it sent a request and when it got
 * an answer, but not when the store actually applied it — only that it happened somewhere
 * in between. Linearizability asks whether there exists a choice of instants, one inside
 * each interval, that explains every result. Recording a single timestamp would throw away
 * exactly the freedom that question is about.
 *
 * @param id unique, for stable ordering and memoisation
 * @param process which client issued it. Operations from one client never overlap.
 * @param kind what it did
 * @param key the key it touched
 * @param writeValue the value written, for PUT
 * @param readValue the value observed, for GET. Null means the key was absent.
 * @param invokedAt when the client sent it
 * @param completedAt when the client got an answer, or {@link Long#MAX_VALUE} if it never
 *     did. A pending operation is not an error: a request lost during a partition may still
 *     have been applied, and a checker that ignored that possibility would report false
 *     violations.
 */
public record Operation(
        long id,
        int process,
        Kind kind,
        String key,
        String writeValue,
        String readValue,
        long invokedAt,
        long completedAt) {

    public enum Kind {
        PUT,
        GET,
        DELETE
    }

    public boolean isPending() {
        return completedAt == Long.MAX_VALUE;
    }

    public static Operation put(
            long id, int process, String key, String value, long invokedAt, long completedAt) {
        return new Operation(id, process, Kind.PUT, key, value, null, invokedAt, completedAt);
    }

    public static Operation get(
            long id, int process, String key, String observed, long invokedAt, long completedAt) {
        return new Operation(id, process, Kind.GET, key, null, observed, invokedAt, completedAt);
    }

    public static Operation delete(
            long id, int process, String key, long invokedAt, long completedAt) {
        return new Operation(id, process, Kind.DELETE, key, null, null, invokedAt, completedAt);
    }

    /** A request that never returned: applied or not, the client cannot tell. */
    public static Operation pending(Operation.Kind kind, long id, int process, String key, String value, long invokedAt) {
        return new Operation(id, process, kind, key, value, null, invokedAt, Long.MAX_VALUE);
    }

    @Override
    public String toString() {
        String detail =
                switch (kind) {
                    case PUT -> "put(%s, %s)".formatted(key, writeValue);
                    case GET -> "get(%s) -> %s".formatted(key, readValue);
                    case DELETE -> "delete(%s)".formatted(key);
                };
        return "p%d %s @[%d, %s]"
                .formatted(process, detail, invokedAt, isPending() ? "pending" : completedAt);
    }
}
