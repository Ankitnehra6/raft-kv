package io.github.ankitnehra6.raftkv.log;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The replicated log.
 *
 * <p>Indices are 1-based to match the paper, and index 0 is the sentinel meaning "before
 * the beginning". Every off-by-one in a Raft implementation lives at this boundary, so the
 * convention is kept identical to the paper rather than translated to 0-based and
 * mentally re-translated at every use.
 *
 * <p>In-memory for now; a durable implementation slots in behind the same operations.
 */
public class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();

    /** Index of the last entry, or 0 when the log is empty. */
    public long lastIndex() {
        return entries.isEmpty() ? 0 : entries.getLast().index();
    }

    /** Term of the last entry, or 0 when the log is empty. */
    public long lastTerm() {
        return entries.isEmpty() ? 0 : entries.getLast().term();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public int size() {
        return entries.size();
    }

    /**
     * Term of the entry at {@code index}, or 0 for index 0.
     *
     * @throws IndexOutOfBoundsException if the index is past the end of the log
     */
    public long termAt(long index) {
        if (index == 0) {
            return 0;
        }
        return entryAt(index)
                .orElseThrow(
                        () ->
                                new IndexOutOfBoundsException(
                                        "no entry at index %d (last is %d)"
                                                .formatted(index, lastIndex())))
                .term();
    }

    public Optional<LogEntry> entryAt(long index) {
        if (index < 1 || index > lastIndex()) {
            return Optional.empty();
        }
        return Optional.of(entries.get(toSlot(index)));
    }

    /**
     * The consistency check from the paper: does this log contain an entry at
     * {@code index} whose term is {@code term}?
     *
     * <p>Index 0 always matches, which is what lets a leader replicate to a follower with
     * an empty log without a special case.
     */
    public boolean matches(long index, long term) {
        if (index == 0) {
            return term == 0;
        }
        return entryAt(index).map(e -> e.term() == term).orElse(false);
    }

    /** Entries from {@code index} to the end, empty if the index is past the end. */
    public List<LogEntry> from(long index) {
        if (index < 1 || index > lastIndex()) {
            return List.of();
        }
        return List.copyOf(entries.subList(toSlot(index), entries.size()));
    }

    /** Appends an entry, which must be exactly one past the current end. */
    public void append(LogEntry entry) {
        long expected = lastIndex() + 1;
        if (entry.index() != expected) {
            throw new IllegalArgumentException(
                    "log must be contiguous: expected index %d, got %d"
                            .formatted(expected, entry.index()));
        }
        entries.add(entry);
    }

    /**
     * Merges entries from a leader, starting at {@code prevIndex + 1}.
     *
     * <p>Existing entries that agree are left alone. The first disagreement truncates
     * everything from that point and the leader's version is taken from there on.
     *
     * <p>Truncating only on an actual conflict matters: a delayed or duplicated
     * AppendEntries can legitimately carry entries the follower already has, and blindly
     * truncating would discard committed entries that a later heartbeat then has to
     * re-send — or worse, discard an entry this node has already acknowledged.
     *
     * @return the index of the last entry now in the log from this batch
     */
    public long appendFrom(long prevIndex, List<LogEntry> incoming) {
        long index = prevIndex;

        for (LogEntry entry : incoming) {
            index = entry.index();

            Optional<LogEntry> existing = entryAt(index);
            if (existing.isPresent()) {
                if (existing.get().term() == entry.term()) {
                    continue; // already have it, and it agrees
                }
                truncateFrom(index); // conflict: this entry and everything after it goes
            }
            entries.add(entry);
        }
        return index;
    }

    /** Removes the entry at {@code index} and everything after it. */
    public void truncateFrom(long index) {
        if (index < 1 || index > lastIndex()) {
            return;
        }
        entries.subList(toSlot(index), entries.size()).clear();
    }

    /** A snapshot of the whole log, for assertions and debugging. */
    public List<LogEntry> entries() {
        return List.copyOf(entries);
    }

    /** Converts a 1-based log index to a 0-based list position. */
    private int toSlot(long index) {
        return (int) (index - 1);
    }

    @Override
    public String toString() {
        return "RaftLog[size=%d, lastIndex=%d, lastTerm=%d]"
                .formatted(entries.size(), lastIndex(), lastTerm());
    }
}
