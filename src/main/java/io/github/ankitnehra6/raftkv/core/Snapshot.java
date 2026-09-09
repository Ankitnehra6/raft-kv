package io.github.ankitnehra6.raftkv.core;

import java.util.Arrays;

/**
 * A state machine's contents at one point in the log, standing in for every entry up to it.
 *
 * <p>Without snapshots the log grows forever: a cluster that has been up for a year must
 * keep every write ever made, a restarting node must replay all of them, and a follower
 * that has been away for an hour must be sent an hour of history. A snapshot replaces that
 * prefix with the state it produced.
 *
 * @param lastIncludedIndex the last log index folded into this snapshot
 * @param lastIncludedTerm that entry's term. Kept because the log no longer holds it, and
 *     the AppendEntries consistency check still needs to be able to ask about that index.
 * @param data the state machine's own serialisation, opaque to Raft
 */
public record Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] data) {

    public Snapshot {
        if (lastIncludedIndex < 0) {
            throw new IllegalArgumentException("lastIncludedIndex must not be negative");
        }
        data = data == null ? new byte[0] : data.clone();
    }

    @Override
    public byte[] data() {
        return data.clone();
    }

    public int sizeBytes() {
        return data.length;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Snapshot other
                && lastIncludedIndex == other.lastIncludedIndex
                && lastIncludedTerm == other.lastIncludedTerm
                && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * Long.hashCode(lastIncludedIndex) + Long.hashCode(lastIncludedTerm))
                + Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "Snapshot[through index %d (term %d), %d bytes]"
                .formatted(lastIncludedIndex, lastIncludedTerm, data.length);
    }
}
