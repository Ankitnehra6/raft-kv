package io.github.ankitnehra6.raftkv.sim;

import io.github.ankitnehra6.raftkv.core.Message;
import io.github.ankitnehra6.raftkv.core.NodeId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.random.RandomGenerator;

/**
 * An in-memory network that can be told to misbehave.
 *
 * <p>Messages are held until a delivery tick chosen from a configurable delay range, so
 * reordering happens naturally rather than having to be staged. Partitions, drops and
 * crashes are all expressed here, which keeps every fault in one place instead of smeared
 * across the tests that need them.
 *
 * <p>Fully deterministic: the same seed produces the same delays, the same drops and the
 * same delivery order, so a failing test reproduces exactly rather than "usually".
 */
public class SimulatedNetwork {

    /** A message waiting for its delivery tick. */
    private record InFlight(Message message, long deliverAt, long sequence) {}

    private final RandomGenerator random;
    private final List<InFlight> inFlight = new ArrayList<>();

    /**
     * Which partition each node is in. Nodes in different groups cannot exchange messages.
     * Everyone starts in group 0, which is a fully connected network.
     */
    private final Map<NodeId, Integer> partitionGroup = new HashMap<>();

    private final Set<NodeId> crashed = new HashSet<>();

    private double dropRate;
    private int minDelayTicks = 1;
    private int maxDelayTicks = 1;

    /** Monotonic, so messages queued in the same tick keep their order deterministically. */
    private long sequence;

    private long delivered;
    private long dropped;

    public SimulatedNetwork(RandomGenerator random) {
        this.random = random;
    }

    // --- configuration --------------------------------------------------------------

    /**
     * Fraction of messages silently discarded, 0.0 to 1.0.
     *
     * <p>Raft has to tolerate loss without any acknowledgement layer beneath it, so this is
     * the cheapest way to check that its retries actually converge.
     */
    public void setDropRate(double rate) {
        if (rate < 0 || rate > 1) {
            throw new IllegalArgumentException("drop rate must be between 0 and 1");
        }
        this.dropRate = rate;
    }

    /**
     * Delivery delay range in ticks.
     *
     * <p>A range rather than a constant, because a constant delay preserves send order and
     * would hide every reordering bug — precisely the class of bug consensus code is prone
     * to.
     */
    public void setDelay(int minTicks, int maxTicks) {
        if (minTicks < 1 || maxTicks < minTicks) {
            throw new IllegalArgumentException("delay range must be positive and ordered");
        }
        this.minDelayTicks = minTicks;
        this.maxDelayTicks = maxTicks;
    }

    /**
     * Splits the cluster so the listed groups cannot talk to each other.
     *
     * <p>In-flight messages that would cross the new boundary are dropped, which is what a
     * real partition does to packets already on the wire.
     */
    @SafeVarargs
    public final void partition(Set<NodeId>... groups) {
        partitionGroup.clear();
        for (int group = 0; group < groups.length; group++) {
            for (NodeId node : groups[group]) {
                partitionGroup.put(node, group);
            }
        }
        inFlight.removeIf(f -> !canReach(f.message().from(), f.message().to()));
    }

    /** Removes every partition, putting all nodes back in one group. */
    public void heal() {
        partitionGroup.clear();
    }

    /** Marks a node as down: it sends and receives nothing until restarted. */
    public void crash(NodeId node) {
        crashed.add(node);
        inFlight.removeIf(f -> f.message().from().equals(node) || f.message().to().equals(node));
    }

    public void restart(NodeId node) {
        crashed.remove(node);
    }

    public boolean isCrashed(NodeId node) {
        return crashed.contains(node);
    }

    /** Which partition group a node is in. Equal numbers can exchange messages. */
    public int partitionGroupOf(NodeId node) {
        return partitionGroup.getOrDefault(node, 0);
    }

    // --- traffic ---------------------------------------------------------------------

    /** Queues a message, unless it is dropped or cannot cross the network as configured. */
    public void send(Message message, long now) {
        if (!canReach(message.from(), message.to())) {
            dropped++;
            return;
        }
        if (dropRate > 0 && random.nextDouble() < dropRate) {
            dropped++;
            return;
        }

        long delay = random.nextInt(minDelayTicks, maxDelayTicks + 1);
        inFlight.add(new InFlight(message, now + delay, sequence++));
    }

    /**
     * Removes and returns everything due at or before {@code now}.
     *
     * <p>Ordered by delivery tick and then by send sequence, so delivery is deterministic
     * even when several messages come due together.
     */
    public List<Message> takeDeliverable(long now) {
        List<InFlight> due =
                inFlight.stream()
                        .filter(f -> f.deliverAt() <= now)
                        .sorted(
                                Comparator.comparingLong(InFlight::deliverAt)
                                        .thenComparingLong(InFlight::sequence))
                        .toList();

        inFlight.removeAll(due);

        List<Message> messages = new ArrayList<>(due.size());
        for (InFlight f : due) {
            // Re-checked at delivery, not only at send: a node may have crashed or a
            // partition may have appeared while the message was in flight.
            if (canReach(f.message().from(), f.message().to())) {
                messages.add(f.message());
                delivered++;
            } else {
                dropped++;
            }
        }
        return messages;
    }

    private boolean canReach(NodeId from, NodeId to) {
        if (crashed.contains(from) || crashed.contains(to)) {
            return false;
        }
        return partitionGroup.getOrDefault(from, 0).equals(partitionGroup.getOrDefault(to, 0));
    }

    public int inFlightCount() {
        return inFlight.size();
    }

    /**
     * Messages currently on the wire, for visualisation.
     *
     * <p>Exposed because a picture of consensus is mostly a picture of what is in flight:
     * a vote request crossing the cluster, heartbeats fanning out, a response arriving one
     * tick too late to matter.
     *
     * @param now the current tick, used to report how far along each message is
     */
    public List<InFlightView> inFlightMessages(long now) {
        return inFlight.stream()
                .map(
                        f ->
                                new InFlightView(
                                        f.message(),
                                        f.deliverAt(),
                                        // 0 at send, 1 at delivery, so a renderer can
                                        // interpolate a position without knowing the delay.
                                        f.deliverAt() <= now
                                                ? 1.0
                                                : 1.0 - (double) (f.deliverAt() - now) / Math.max(1, maxDelayTicks)))
                .toList();
    }

    /**
     * @param message the message in flight
     * @param deliverAt the tick it lands
     * @param progress 0.0 at send through 1.0 at delivery
     */
    public record InFlightView(Message message, long deliverAt, double progress) {}

    public long deliveredCount() {
        return delivered;
    }

    public long droppedCount() {
        return dropped;
    }
}
