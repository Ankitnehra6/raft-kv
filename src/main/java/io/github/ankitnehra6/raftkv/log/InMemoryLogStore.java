package io.github.ankitnehra6.raftkv.log;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import java.util.ArrayList;
import java.util.List;

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
        if (index < 1 || index > lastIndex()) {
            return;
        }
        entries.subList((int) (index - 1), entries.size()).clear();
    }

    @Override
    public List<LogEntry> readAll() {
        return List.copyOf(entries);
    }

    @Override
    public long lastIndex() {
        return entries.isEmpty() ? 0 : entries.getLast().index();
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
    public void close() {
        // Nothing to release.
    }
}
