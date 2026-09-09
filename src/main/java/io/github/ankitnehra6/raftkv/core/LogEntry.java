package io.github.ankitnehra6.raftkv.core;

import java.util.Arrays;

/**
 * One entry in the replicated log.
 *
 * @param term the term in which the leader created this entry. Carried per entry, not per
 *     log, because the consistency check compares the term at a specific index — that is
 *     what lets a follower detect a divergent suffix rather than silently accepting it.
 * @param index position in the log, 1-based, matching the paper
 * @param command opaque bytes handed to the state machine once committed. Raft has no
 *     opinion on what a command means, which is what makes the same implementation usable
 *     for a key-value store or anything else.
 */
public record LogEntry(long term, long index, Type type, byte[] command) {

    /**
     * What an entry means to the layers above Raft.
     *
     * <p>An explicit type rather than inferring from an empty command. That worked while
     * no-ops were the only special case, but a configuration entry also has to be
     * distinguishable, and "empty means no-op" would silently misread one.
     */
    public enum Type {
        /** An opaque command for the state machine. */
        COMMAND,
        /** A new leader's entry in its own term, committing nothing. */
        NOOP,
        /** A cluster membership change, applied by Raft rather than the state machine. */
        CONFIGURATION
    }

    /** Convenience for the common case. */
    public LogEntry(long term, long index, byte[] command) {
        this(term, index, Type.COMMAND, command);
    }

    public LogEntry {
        if (term < 0) {
            throw new IllegalArgumentException("term must not be negative");
        }
        if (index < 1) {
            throw new IllegalArgumentException("log index is 1-based; got " + index);
        }
        command = command == null ? new byte[0] : command.clone();
    }

    @Override
    public byte[] command() {
        // Defensive copy: a committed entry is handed to the state machine and may be
        // replayed from a snapshot, so nothing outside should be able to mutate it.
        return command.clone();
    }

    /** A no-op entry, used by a new leader to commit its own term. */
    public static LogEntry noop(long term, long index) {
        return new LogEntry(term, index, Type.NOOP, new byte[0]);
    }

    /** A membership change. */
    public static LogEntry configuration(long term, long index, byte[] encodedConfig) {
        return new LogEntry(term, index, Type.CONFIGURATION, encodedConfig);
    }

    public boolean isNoop() {
        return type == Type.NOOP;
    }

    public boolean isConfiguration() {
        return type == Type.CONFIGURATION;
    }

    /** Whether this entry is meant for the state machine at all. */
    public boolean isCommand() {
        return type == Type.COMMAND;
    }

    // Records compare arrays by reference, which would make two identical entries unequal
    // and break log comparisons in tests. Value semantics are what callers expect here.

    @Override
    public boolean equals(Object o) {
        return o instanceof LogEntry other
                && term == other.term
                && index == other.index
                && type == other.type
                && Arrays.equals(command, other.command);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * (31 * Long.hashCode(term) + Long.hashCode(index)) + type.hashCode())
                + Arrays.hashCode(command);
    }

    @Override
    public String toString() {
        return "LogEntry[term=%d, index=%d, %s, bytes=%d]"
                .formatted(term, index, type, command.length);
    }
}
