package io.github.ankitnehra6.raftkv.log;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import io.github.ankitnehra6.raftkv.core.Snapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A {@link LogStore} that keeps everything in memory.
 *
 * <p>Used by the simulation, where thousands of clusters are created and destroyed per
 * second and touching a disk would dominate the run time. It is also what lets a simulated
 * "crash and restart" be modelled honestly: the store survives the node, exactly as a real
 * disk would, so a restarted node recovers its term, vote and log rather than starting
 * blank.
 */
public class InMemoryLogStore implements LogStore {

    private final List<LogEntry> entries = new ArrayList<>();
    private PersistentState state = PersistentState.INITIAL;
    private Snapshot snapshot;

    /**
     * Index of the first entry still held. Rises as snapshots compact the front away, so
     * the list stays a suffix of the logical log rather than being re-indexed.
     */
    private long firstIndex = 1;

    @Override
    public void append(List<LogEntry> incoming) {
        for (LogEntry entry : incoming) {
            long expected = lastIndex() + 1;
            if (entry.index() != expected) {
                throw new IllegalArgumentException(
                        "log must be contiguous: expected %d, got %d"
                                .formatted(expected, entry.index()));
            }
            entries.add(entry);
        }
    }

    @Override
    public void truncateFrom(long index) {
        if (index < firstIndex || index > lastIndex()) {
            return;
        }
        entries.subList((int) (index - firstIndex), entries.size()).clear();
    }

    @Override
    public List<LogEntry> readAll() {
        return List.copyOf(entries);
    }

    @Override
    public long lastIndex() {
        if (!entries.isEmpty()) {
            return entries.getLast().index();
        }
        return snapshot == null ? 0 : snapshot.lastIncludedIndex();
    }

    @Override
    public void saveState(PersistentState state) {
        this.state = state;
    }

    @Override
    public PersistentState loadState() {
        return state;
    }

    @Override
    public void saveSnapshot(Snapshot toSave) {
        this.snapshot = toSave;
        // Drop the prefix the snapshot now covers.
        entries.removeIf(entry -> entry.index() <= toSave.lastIncludedIndex());
        firstIndex = toSave.lastIncludedIndex() + 1;
    }

    @Override
    public Optional<Snapshot> loadSnapshot() {
        return Optional.ofNullable(snapshot);
    }

    @Override
    public void close() {
        // Nothing to release.
    }
}
