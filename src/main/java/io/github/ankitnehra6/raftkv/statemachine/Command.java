package io.github.ankitnehra6.raftkv.statemachine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A write against the key-value store.
 *
 * <p>Commands are encoded to bytes before entering the log, because Raft has no opinion on
 * what a command means — that opacity is what lets the same consensus code replicate
 * anything.
 *
 * <p>The encoding is length-prefixed rather than delimiter-separated so keys and values may
 * contain any bytes at all. A delimiter scheme would work until the first key containing
 * the delimiter, and would then corrupt the log rather than fail loudly.
 */
public sealed interface Command {

    byte PUT = 1;
    byte DELETE = 2;

    record Put(String key, String value) implements Command {}

    record Delete(String key) implements Command {}

    /** Encodes a command for the replicated log. */
    static byte[] encode(Command command) {
        return switch (command) {
            case Put(String key, String value) -> {
                byte[] k = key.getBytes(StandardCharsets.UTF_8);
                byte[] v = value.getBytes(StandardCharsets.UTF_8);
                yield ByteBuffer.allocate(1 + 4 + k.length + 4 + v.length)
                        .put(PUT)
                        .putInt(k.length)
                        .put(k)
                        .putInt(v.length)
                        .put(v)
                        .array();
            }
            case Delete(String key) -> {
                byte[] k = key.getBytes(StandardCharsets.UTF_8);
                yield ByteBuffer.allocate(1 + 4 + k.length).put(DELETE).putInt(k.length).put(k).array();
            }
        };
    }

    /**
     * Decodes a command from the log.
     *
     * @throws IllegalArgumentException if the bytes are not a command this version
     *     understands. Failing loudly matters: a state machine that silently skips an entry
     *     it cannot parse diverges from its peers, which is the one thing replication
     *     exists to prevent.
     */
    static Command decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte type = buffer.get();

        return switch (type) {
            case PUT -> new Put(readString(buffer), readString(buffer));
            case DELETE -> new Delete(readString(buffer));
            default -> throw new IllegalArgumentException("unknown command type: " + type);
        };
    }

    private static String readString(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0 || length > buffer.remaining()) {
            throw new IllegalArgumentException("malformed command: bad length " + length);
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
