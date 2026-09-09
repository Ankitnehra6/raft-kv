package io.github.ankitnehra6.raftkv.sim;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import io.github.ankitnehra6.raftkv.core.Message;
import io.github.ankitnehra6.raftkv.core.NodeId;
import io.github.ankitnehra6.raftkv.core.RaftConfig;
import io.github.ankitnehra6.raftkv.core.RaftNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;
import java.util.stream.Collectors;

/**
 * A whole Raft cluster running single-threaded in logical time.
 *
 * <p>One {@link #tick()} advances every node, moves the network forward, and delivers
 * whatever is due — so an hour of cluster behaviour costs a millisecond and happens in a
 * defined order. Given a seed, a run is exactly reproducible, which turns "this test fails
 * about one time in fifty" into a fixed input someone can debug.
 *
 * <p>This is the harness the consensus code was written against, not something bolted on
 * afterwards.
 */
public class SimulatedCluster {

    /**
     * A splittable, well-distributed generator. Named explicitly rather than using
     * {@code new Random(seed)} so the stream is specified by the JDK and a run reproduces
     * across JDK versions.
     */
    private static final RandomGeneratorFactory<RandomGenerator> RANDOM_FACTORY =
            RandomGeneratorFactory.of("L64X128MixRandom");

    private final Map<NodeId, RaftNode> nodes = new LinkedHashMap<>();
    private final Map<NodeId, List<LogEntry>> applied = new LinkedHashMap<>();
    private final SimulatedNetwork network;
    private final RandomGenerator random;
    private final RaftConfig config;

    private long tick;

    public SimulatedCluster(int size, long seed) {
        this(size, seed, RaftConfig.defaults());
    }

    public SimulatedCluster(int size, long seed, RaftConfig config) {
        if (size < 1) {
            throw new IllegalArgumentException("cluster needs at least one node");
        }
        this.config = config;
        this.random = RANDOM_FACTORY.create(seed);
        this.network = new SimulatedNetwork(random);

        List<NodeId> ids =
                java.util.stream.IntStream.range(0, size)
                        .mapToObj(i -> NodeId.of("n" + i))
                        .toList();

        for (NodeId id : ids) {
            Set<NodeId> peers = ids.stream().filter(other -> !other.equals(id)).collect(Collectors.toSet());
            // Each node gets its own random stream, derived from the cluster seed. Sharing
            // one generator would make a node's election timeout depend on how many random
            // draws its peers happened to make first, so adding a node would perturb
            // everyone's timing and no scenario would stay reproducible across changes.
            nodes.put(
                    id,
                    new RaftNode(
                            id, peers, config, RANDOM_FACTORY.create(seed ^ id.value().hashCode())));
            applied.put(id, new ArrayList<>());
        }
    }

    // --- driving ---------------------------------------------------------------------

    /** Advances logical time by one tick across the whole cluster. */
    public void tick() {
        tick++;

        // Every live node ticks before any message is delivered, so a tick is a coherent
        // instant rather than a sequence in which earlier nodes see later nodes' output.
        for (RaftNode node : nodes.values()) {
            if (!network.isCrashed(node.id())) {
                node.tick();
            }
        }

        for (RaftNode node : nodes.values()) {
            if (network.isCrashed(node.id())) {
                // A crashed node's outbound queue is discarded: it never got to send.
                node.drainOutbound();
                continue;
            }
            for (Message message : node.drainOutbound()) {
                network.send(message, tick);
            }
            applied.get(node.id()).addAll(node.drainCommitted());
        }

        for (Message message : network.takeDeliverable(tick)) {
            nodes.get(message.to()).receive(message);
        }
    }

    public void tick(int count) {
        for (int i = 0; i < count; i++) {
            tick();
        }
    }

    /**
     * Ticks until {@code condition} holds, up to a bound.
     *
     * @return true if the condition became true within the budget
     */
    public boolean tickUntil(int maxTicks, java.util.function.BooleanSupplier condition) {
        for (int i = 0; i < maxTicks; i++) {
            if (condition.getAsBoolean()) {
                return true;
            }
            tick();
        }
        return condition.getAsBoolean();
    }

    /** Ticks until a single leader is established, or the budget runs out. */
    public boolean tickUntilLeaderElected(int maxTicks) {
        return tickUntil(maxTicks, () -> leader().isPresent());
    }

    // --- inspection --------------------------------------------------------------------

    /**
     * The current leader, if exactly one node believes it leads in the highest term.
     *
     * <p>Empty when there is none, and also when there are several — during a partition two
     * nodes can both consider themselves leader, and reporting one of them arbitrarily
     * would hide exactly the situation worth noticing.
     */
    public Optional<RaftNode> leader() {
        List<RaftNode> leaders = liveNodes().stream().filter(RaftNode::isLeader).toList();
        if (leaders.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(leaders.getFirst());
    }

    /** Every node that currently believes it is leader, however many that is. */
    public List<RaftNode> allLeaders() {
        return liveNodes().stream().filter(RaftNode::isLeader).toList();
    }

    public RaftNode node(NodeId id) {
        RaftNode node = nodes.get(id);
        if (node == null) {
            throw new IllegalArgumentException("no such node: " + id);
        }
        return node;
    }

    public RaftNode node(String id) {
        return node(NodeId.of(id));
    }

    public List<RaftNode> nodes() {
        return List.copyOf(nodes.values());
    }

    public List<RaftNode> liveNodes() {
        return nodes.values().stream().filter(n -> !network.isCrashed(n.id())).toList();
    }

    /** Entries the given node has applied to its state machine, in order. */
    public List<LogEntry> appliedAt(NodeId id) {
        return List.copyOf(applied.get(id));
    }

    public List<LogEntry> appliedAt(String id) {
        return appliedAt(NodeId.of(id));
    }

    public long currentTick() {
        return tick;
    }

    public SimulatedNetwork network() {
        return network;
    }

    public RaftConfig config() {
        return config;
    }

    // --- faults ---------------------------------------------------------------------

    @SafeVarargs
    public final void partition(Set<NodeId>... groups) {
        network.partition(groups);
    }

    /** Convenience: partition by node name. */
    public void partition(List<String> groupA, List<String> groupB) {
        network.partition(
                groupA.stream().map(NodeId::of).collect(Collectors.toSet()),
                groupB.stream().map(NodeId::of).collect(Collectors.toSet()));
    }

    public void heal() {
        network.heal();
    }

    public void crash(String id) {
        network.crash(NodeId.of(id));
    }

    public void restart(String id) {
        network.restart(NodeId.of(id));
    }

    public void setDropRate(double rate) {
        network.setDropRate(rate);
    }

    public void setDelay(int minTicks, int maxTicks) {
        network.setDelay(minTicks, maxTicks);
    }

    @Override
    public String toString() {
        return "tick=%d %s".formatted(tick, nodes.values());
    }
}
