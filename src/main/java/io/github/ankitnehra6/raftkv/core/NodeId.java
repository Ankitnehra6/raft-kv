package io.github.ankitnehra6.raftkv.core;

/**
 * A stable identifier for one member of the cluster.
 *
 * <p>A wrapper rather than a bare {@code String} because node ids and keys and commands are
 * all strings, and Raft code passes a lot of them around. The compiler catching a
 * transposed argument is worth the four lines.
 */
public record NodeId(String value) implements Comparable<NodeId> {

    public NodeId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public int compareTo(NodeId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
