package io.github.ankitnehra6.raftkv.log;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import io.github.ankitnehra6.raftkv.core.Snapshot;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * Durable storage for the log and the state that must outlive a restart.
 *
 * <p>An interface so the algorithm can be simulation-tested against an in-memory
 * implementation and run against a file-backed one, without knowing which it has.
 */
public interface LogStore extends Closeable {

    /**
     * Appends entries and makes them durable before returning.
     *
     * <p>Durability before returning is the whole contract. Raft only counts a follower as
     * having stored an entry once it acknowledges it, and a follower that acknowledges
     * something still sitting in a page cache can lose it in a power failure — after the
     * leader has already told a client the write succeeded.
     */
    void append(List<LogEntry> entries);

    /** Removes the entry at {@code index} and everything after it. */
    void truncateFrom(long index);

    /** Every entry, in order. Used to rebuild in-memory state on startup. */
    List<LogEntry> readAll();

    long lastIndex();

    /** Persists term and vote. Must be durable before returning, for the same reason. */
    void saveState(PersistentState state);

    PersistentState loadState();

    /**
     * Stores a snapshot and discards the log prefix it covers.
     *
     * <p>Written before the entries are dropped, and durable before returning. The reverse
     * order — trim first, then write — has a window in which a crash leaves neither the
     * entries nor the snapshot, which loses committed state outright.
     */
    void saveSnapshot(Snapshot snapshot);

    Optional<Snapshot> loadSnapshot();
}
