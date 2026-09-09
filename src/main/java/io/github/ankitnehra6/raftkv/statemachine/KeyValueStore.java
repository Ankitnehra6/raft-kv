package io.github.ankitnehra6.raftkv.statemachine;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The replicated state machine: a map, built by applying committed log entries in order.
 *
 * <p>Deliberately trivial. The interesting property is not what it stores but that every
 * replica reaches the same contents, and it reaches them purely by replaying the same
 * sequence — no clocks, no randomness, no I/O. A state machine that consulted anything
 * outside its input would diverge between replicas and quietly break the guarantee the
 * consensus layer works so hard to provide.
 */
public class KeyValueStore {

    private final Map<String, String> data = new LinkedHashMap<>();
    private long lastAppliedIndex;

    /**
     * Applies one committed entry.
     *
     * <p>No-ops are skipped: a new leader appends one to commit its own term, and it
     * carries no meaning for the state machine.
     *
     * @return the value observed, for a read; empty for a write or a no-op. Returned from
     *     {@code apply} rather than read separately afterwards, because the answer a read
     *     must give is the state <em>at its position in the log</em>, and any later lookup
     *     could already reflect a write that came after it.
     */
    public Optional<String> apply(LogEntry entry) {
        if (entry.index() <= lastAppliedIndex) {
            // Applying an entry twice would make non-idempotent commands wrong. Entries
            // arrive in order, so anything at or below the high-water mark is a replay.
            return Optional.empty();
        }
        lastAppliedIndex = entry.index();

        if (!entry.isCommand()) {
            // No-ops and configuration changes mean nothing here: the first commits a
            // leader's term, the second is applied by Raft itself.
            return Optional.empty();
        }

        return switch (Command.decode(entry.command())) {
            case Command.Put(String key, String value) -> {
                data.put(key, value);
                yield Optional.empty();
            }
            case Command.Delete(String key) -> {
                data.remove(key);
                yield Optional.empty();
            }
            // Wrapped so an absent key is distinguishable from "not a read" by the caller,
            // which tracks reads by log index rather than by inspecting the return.
            case Command.Get(String key) -> Optional.ofNullable(data.get(key));
        };
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    public boolean containsKey(String key) {
        return data.containsKey(key);
    }

    public int size() {
        return data.size();
    }

    /** Index of the last entry applied, for snapshot bookkeeping. */
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /** A snapshot of the contents, for assertions and for comparing replicas. */
    public Map<String, String> snapshot() {
        return Map.copyOf(data);
    }

    /**
     * Serialises the whole map for a snapshot.
     *
     * <p>Length-prefixed, like commands, so keys and values may contain any bytes. The
     * applied index is <em>not</em> written: it is already recorded on the snapshot itself,
     * and storing it twice invites the two copies to disagree.
     */
    public byte[] snapshotBytes() {
        int size = 4;
        for (Map.Entry<String, String> entry : data.entrySet()) {
            size += 4 + utf8(entry.getKey()).length + 4 + utf8(entry.getValue()).length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(size).putInt(data.size());
        for (Map.Entry<String, String> entry : data.entrySet()) {
            byte[] key = utf8(entry.getKey());
            byte[] value = utf8(entry.getValue());
            buffer.putInt(key.length).put(key).putInt(value.length).put(value);
        }
        return buffer.array();
    }

    /**
     * Replaces the entire contents from a snapshot.
     *
     * <p>The existing map is cleared rather than merged. A snapshot is the complete state at
     * its index, so anything left over would be state the snapshot says does not exist —
     * typically a key the leader deleted while this node was away.
     */
    public void restore(byte[] snapshot, long lastIncludedIndex) {
        data.clear();

        ByteBuffer buffer = ByteBuffer.wrap(snapshot);
        int count = buffer.getInt();
        if (count < 0) {
            throw new IllegalArgumentException("malformed snapshot: negative entry count");
        }

        for (int i = 0; i < count; i++) {
            data.put(readString(buffer), readString(buffer));
        }
        lastAppliedIndex = lastIncludedIndex;
    }

    private static String readString(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0 || length > buffer.remaining()) {
            throw new IllegalArgumentException("malformed snapshot: bad length " + length);
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "KeyValueStore[keys=%d, lastApplied=%d]".formatted(data.size(), lastAppliedIndex);
    }
}
