package io.github.ankitnehra6.raftkv.core;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Who is in the cluster.
 *
 * <p>Membership lives in the log rather than in a config file, because every node has to
 * agree on when it changed relative to every other operation. A file could be edited on one
 * machine and not another, and the two would compute different majorities — which is
 * precisely how a cluster ends up with two leaders that both believe they have one.
 *
 * @param members every voting server, including the local node
 */
public record ClusterConfig(Set<NodeId> members) {

    public ClusterConfig {
        if (members == null || members.isEmpty()) {
            throw new IllegalArgumentException("a cluster needs at least one member");
        }
        members = Set.copyOf(members);
    }

    public static ClusterConfig of(NodeId... members) {
        return new ClusterConfig(Set.of(members));
    }

    public int size() {
        return members.size();
    }

    public boolean contains(NodeId node) {
        return members.contains(node);
    }

    /** Everyone except the given node. */
    public Set<NodeId> peersOf(NodeId self) {
        return members.stream().filter(m -> !m.equals(self)).collect(Collectors.toSet());
    }

    /** Whether a count of votes or replicas constitutes a majority of this configuration. */
    public boolean isMajority(int count) {
        return count * 2 > members.size();
    }

    public ClusterConfig with(NodeId node) {
        Set<NodeId> updated = new LinkedHashSet<>(members);
        updated.add(node);
        return new ClusterConfig(updated);
    }

    public ClusterConfig without(NodeId node) {
        Set<NodeId> updated = new LinkedHashSet<>(members);
        updated.remove(node);
        return new ClusterConfig(updated);
    }

    /** Encodes for a configuration log entry: node ids separated by a comma. */
    public byte[] encode() {
        return members.stream()
                .map(NodeId::value)
                .sorted() // deterministic, so identical configurations encode identically
                .collect(Collectors.joining(","))
                .getBytes(StandardCharsets.UTF_8);
    }

    public static ClusterConfig decode(byte[] encoded) {
        String joined = new String(encoded, StandardCharsets.UTF_8);
        if (joined.isBlank()) {
            throw new IllegalArgumentException("configuration entry is empty");
        }
        return new ClusterConfig(
                Arrays.stream(joined.split(",")).map(NodeId::of).collect(Collectors.toSet()));
    }

    @Override
    public String toString() {
        return members.stream().map(NodeId::value).sorted().collect(Collectors.joining(",", "[", "]"));
    }
}
