package io.github.ankitnehra6.raftkv.statemachine;

import io.github.ankitnehra6.raftkv.core.LogEntry;
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
     */
    public void apply(LogEntry entry) {
        if (entry.index() <= lastAppliedIndex) {
            // Applying an entry twice would make non-idempotent commands wrong. Entries
            // arrive in order, so anything at or below the high-water mark is a replay.
            return;
        }
        lastAppliedIndex = entry.index();

        if (entry.isNoop()) {
            return;
        }

        switch (Command.decode(entry.command())) {
            case Command.Put(String key, String value) -> data.put(key, value);
            case Command.Delete(String key) -> data.remove(key);
        }
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

    @Override
    public String toString() {
        return "KeyValueStore[keys=%d, lastApplied=%d]".formatted(data.size(), lastAppliedIndex);
    }
}
