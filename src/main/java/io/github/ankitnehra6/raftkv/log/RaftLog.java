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
 * <p>Backed by a {@link LogStore}, which is what makes the entries durable. The in-memory
 * list is a cache of what the store holds: every mutation is written through before the
 * method returns, so a crash can never leave the store behind the cache. The other order —
 * cache first, disk later — is what turns an acknowledged write into a lost one.
 */
public class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private final LogStore store;

    /**
     * The last index folded into a snapshot, and its term. Everything at or below this is
     * no longer in {@code entries}.
     *
     * <p>The term has to be kept separately because the entry that carried it is gone, and
     * the AppendEntries consistency check can still legitimately ask about that index — a
     * follower one entry behind the snapshot boundary is the normal case, not an edge one.
     */
    private long snapshotIndex;

    private long snapshotTerm;

    /** A volatile log, for tests that do not care about durability. */
    public RaftLog() {
        this(new InMemoryLogStore());
    }

    /** Opens a log over a store, recovering whatever it already holds. */
    public RaftLog(LogStore store) {
        this.store = store;
        store.loadSnapshot()
                .ifPresent(
                        snapshot -> {
                            snapshotIndex = snapshot.lastIncludedIndex();
                            snapshotTerm = snapshot.lastIncludedTerm();
                        });
        entries.addAll(store.readAll());
    }

    /** The last index covered by a snapshot; 0 if there is none. */
    public long snapshotIndex() {
        return snapshotIndex;
    }

    public long snapshotTerm() {
        return snapshotTerm;
    }

    /** The store beneath, so a node can persist its term and vote through the same handle. */
    public LogStore store() {
        return store;
    }

    /** Index of the last entry, or the snapshot boundary when no entries remain. */
    public long lastIndex() {
        return entries.isEmpty() ? snapshotIndex : entries.getLast().index();
    }

    /** Term of the last entry, or the snapshot's term when no entries remain. */
    public long lastTerm() {
        return entries.isEmpty() ? snapshotTerm : entries.getLast().term();
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
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        return entryAt(index)
                .orElseThrow(
                        () ->
                                new IndexOutOfBoundsException(
                                        "no entry at index %d (last is %d)"
                                                .formatted(index, lastIndex())))
                .term();
    }

    /**
     * The entry at {@code index}, if it is still held.
     *
     * <p>Empty for anything at or below the snapshot boundary: that entry existed, but its
     * contents have been folded into the snapshot and discarded.
     */
    public Optional<LogEntry> entryAt(long index) {
        if (index <= snapshotIndex || index > lastIndex()) {
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
        if (index == snapshotIndex) {
            return term == snapshotTerm;
        }
        return entryAt(index).map(e -> e.term() == term).orElse(false);
    }

    /** Entries from {@code index} to the end, empty if the index is past the end. */
    /**
     * Entries from {@code index} to the end.
     *
     * <p>Empty when the index is below the snapshot boundary — those entries no longer
     * exist, and the leader must send a snapshot instead of trying to replicate them.
     * {@link #hasEntriesFrom} is how a caller tells the two empty cases apart.
     */
    public List<LogEntry> from(long index) {
        if (index <= snapshotIndex || index > lastIndex()) {
            return List.of();
        }
        return List.copyOf(entries.subList(toSlot(index), entries.size()));
    }

    /** Whether {@code index} is still replicable from the log rather than only a snapshot. */
    public boolean hasEntriesFrom(long index) {
        return index > snapshotIndex;
    }

    /**
     * Appends an entry, which must be exactly one past the current end. After compaction
     * that end is the snapshot boundary, not zero.
     */
    public void append(LogEntry entry) {
        long expected = lastIndex() + 1;
        if (entry.index() != expected) {
            throw new IllegalArgumentException(
                    "log must be contiguous: expected index %d, got %d"
                            .formatted(expected, entry.index()));
        }
        // Durable before it is visible: a caller that sees this return may acknowledge the
        // entry to a leader, and an acknowledgement of something not yet on disk is a lie.
        store.append(List.of(entry));
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
            store.append(List.of(entry));
            entries.add(entry);
        }
        return index;
    }

    /** Removes the entry at {@code index} and everything after it. */
    public void truncateFrom(long index) {
        if (index < 1 || index > lastIndex()) {
            return;
        }
        store.truncateFrom(index);
        entries.subList(toSlot(index), entries.size()).clear();
    }

    /**
     * Discards every entry up to and including {@code index}, which a snapshot now covers.
     *
     * <p>Refuses to compact past the end of the log, and refuses to go backwards: a stale
     * compaction request arriving late must not resurrect entries that were already folded
     * away.
     */
    public void compactTo(long index, long term) {
        if (index <= snapshotIndex) {
            return;
        }
        if (index > lastIndex()) {
            throw new IllegalArgumentException(
                    "cannot compact to %d, past the end of the log (%d)".formatted(index, lastIndex()));
        }

        int keepFrom = toSlot(index) + 1;
        entries.subList(0, keepFrom).clear();
        snapshotIndex = index;
        snapshotTerm = term;
    }

    /**
     * Replaces the entire log with a snapshot, discarding whatever was there.
     *
     * <p>Used when a follower is so far behind that the leader sends state rather than
     * entries. Everything local goes: the leader's snapshot is authoritative, and any local
     * entry beyond it was, by definition, never committed.
     */
    public void resetToSnapshot(long index, long term) {
        entries.clear();
        snapshotIndex = index;
        snapshotTerm = term;
    }

    /** A snapshot of the whole log, for assertions and debugging. */
    public List<LogEntry> entries() {
        return List.copyOf(entries);
    }

    /**
     * Converts a 1-based log index to a 0-based list position, accounting for entries the
     * snapshot has removed from the front.
     */
    private int toSlot(long index) {
        return (int) (index - snapshotIndex - 1);
    }

    @Override
    public String toString() {
        return "RaftLog[size=%d, lastIndex=%d, lastTerm=%d, snapshotIndex=%d]"
                .formatted(entries.size(), lastIndex(), lastTerm(), snapshotIndex);
    }
}
