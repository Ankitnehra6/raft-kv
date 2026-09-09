package io.github.ankitnehra6.raftkv.server;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import io.github.ankitnehra6.raftkv.core.Message;
import io.github.ankitnehra6.raftkv.core.NodeId;
import io.github.ankitnehra6.raftkv.core.RaftConfig;
import io.github.ankitnehra6.raftkv.core.RaftNode;
import io.github.ankitnehra6.raftkv.core.Role;
import io.github.ankitnehra6.raftkv.grpc.AppendEntriesReply;
import io.github.ankitnehra6.raftkv.grpc.AppendEntriesRequest;
import io.github.ankitnehra6.raftkv.grpc.InstallSnapshotReply;
import io.github.ankitnehra6.raftkv.grpc.InstallSnapshotRequest;
import io.github.ankitnehra6.raftkv.grpc.RaftGrpc;
import io.github.ankitnehra6.raftkv.grpc.RequestVoteReply;
import io.github.ankitnehra6.raftkv.grpc.RequestVoteRequest;
import io.github.ankitnehra6.raftkv.log.InMemoryLogStore;
import io.github.ankitnehra6.raftkv.log.LogStore;
import io.github.ankitnehra6.raftkv.statemachine.KeyValueStore;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.random.RandomGeneratorFactory;

/**
 * A real Raft server: the pure core driven by sockets and a clock instead of a simulation.
 *
 * <p>The algorithm is untouched. {@link RaftNode} still has no threads, no I/O and no clock;
 * everything below supplies those. That the same class is driven by both this and
 * {@code SimulatedCluster} is the payoff of the design — the exhaustive fault testing done
 * in simulation applies to what actually runs here.
 *
 * <p><b>Threading.</b> {@code RaftNode} is not thread-safe, so every touch of it happens on
 * one event-loop thread. gRPC handlers, the ticker and client requests all hand work to that
 * thread and wait for a result. The loop never performs network I/O: outbound RPCs are
 * dispatched to a separate pool and their replies fed back in, because blocking the loop on
 * an unreachable peer would stop heartbeats to every other peer and cause the very election
 * the RPC was meant to prevent.
 */
public class RaftServer implements AutoCloseable {

    private static final Logger log = System.getLogger(RaftServer.class.getName());

    /** Real time per logical tick. Elections then fall between 500ms and 1s by default. */
    private static final long TICK_MILLIS = 50;

    private final NodeId id;
    private final RaftNode node;
    private final KeyValueStore stateMachine = new KeyValueStore();
    private final Map<NodeId, String> peerAddresses;

    private final Server grpcServer;
    private final Map<NodeId, ManagedChannel> channels = new HashMap<>();
    private final Map<NodeId, RaftGrpc.RaftBlockingStub> stubs = new HashMap<>();

    /** Serialises every access to the node. */
    private final ExecutorService loop =
            Executors.newSingleThreadExecutor(r -> Thread.ofPlatform().name("raft-loop").unstarted(r));

    /** Outbound RPCs, kept off the loop so a dead peer cannot stall the whole node. */
    private final ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor();

    private final ScheduledExecutorService ticker =
            Executors.newSingleThreadScheduledExecutor(
                    r -> Thread.ofPlatform().name("raft-tick").unstarted(r));

    /** Client requests awaiting the log entry they proposed. */
    private final Map<Long, PendingRequest> pending = new ConcurrentHashMap<>();

    private record PendingRequest(byte[] command, CompletableFuture<Optional<String>> result) {}

    private volatile boolean running;

    public RaftServer(NodeId id, Map<NodeId, String> peerAddresses, int port) {
        this(id, peerAddresses, port, new InMemoryLogStore(), RaftConfig.defaults());
    }

    public RaftServer(
            NodeId id,
            Map<NodeId, String> peerAddresses,
            int port,
            LogStore store,
            RaftConfig config) {

        this.id = id;
        this.peerAddresses = Map.copyOf(peerAddresses);
        this.node =
                new RaftNode(
                        id,
                        peerAddresses.keySet(),
                        config,
                        RandomGeneratorFactory.of("L64X128MixRandom").create(id.value().hashCode()),
                        store);

        this.grpcServer =
                NettyServerBuilder.forPort(port)
                        .addService(new RaftService())
                        .addService(new KeyValueService(this))
                        .build();
    }

    public void start() throws IOException {
        grpcServer.start();
        running = true;

        for (Map.Entry<NodeId, String> peer : peerAddresses.entrySet()) {
            ManagedChannel channel =
                    NettyChannelBuilder.forTarget(peer.getValue()).usePlaintext().build();
            channels.put(peer.getKey(), channel);
            stubs.put(peer.getKey(), RaftGrpc.newBlockingStub(channel));
        }

        ticker.scheduleAtFixedRate(
                () -> onLoop(this::tick), TICK_MILLIS, TICK_MILLIS, TimeUnit.MILLISECONDS);

        log.log(Level.INFO, "%s listening on %d, peers %s".formatted(id, port(), peerAddresses.keySet()));
    }

    public int port() {
        return grpcServer.getPort();
    }

    public NodeId id() {
        return id;
    }

    // --- the event loop ---------------------------------------------------------------

    /** Runs work on the loop thread and waits for it, so callers see a consistent node. */
    private <T> T onLoop(java.util.function.Supplier<T> work) {
        if (!running) {
            throw new IllegalStateException("server is not running");
        }
        try {
            return CompletableFuture.supplyAsync(work, loop).join();
        } catch (Exception e) {
            throw new IllegalStateException("event loop task failed", e);
        }
    }

    private void onLoop(Runnable work) {
        onLoop(
                () -> {
                    work.run();
                    return null;
                });
    }

    private void tick() {
        node.tick();
        applyAndDispatch();
    }

    /**
     * Applies whatever committed and sends whatever the node queued.
     *
     * <p>Called after every interaction with the node, because the node produces its
     * outputs synchronously and expects the driver to collect them.
     */
    private void applyAndDispatch() {
        node.takeSnapshotToRestore()
                .ifPresent(snapshot -> stateMachine.restore(snapshot.data(), snapshot.lastIncludedIndex()));

        for (LogEntry entry : node.drainCommitted()) {
            Optional<String> result = stateMachine.apply(entry);

            PendingRequest waiting = pending.remove(entry.index());
            if (waiting == null) {
                continue;
            }
            if (java.util.Arrays.equals(waiting.command(), entry.command())) {
                waiting.result().complete(result);
            } else {
                // A different leader's entry took this index; the client's proposal was
                // never committed and it must retry rather than be told a wrong answer.
                waiting.result().completeExceptionally(new NotLeaderException(leaderHint()));
            }
        }

        dispatch(node.drainOutbound());
    }

    /** Sends outbound requests, off the loop thread. */
    private void dispatch(List<Message> outbound) {
        for (Message message : outbound) {
            switch (message) {
                case Message.RequestVote m -> senders.submit(() -> sendRequestVote(m));
                case Message.AppendEntries m -> senders.submit(() -> sendAppendEntries(m));
                case Message.InstallSnapshot m -> senders.submit(() -> sendInstallSnapshot(m));
                // Responses are returned as the gRPC reply by whoever handled the request,
                // never sent as a fresh call.
                default -> {}
            }
        }
    }

    private void sendRequestVote(Message.RequestVote m) {
        try {
            RequestVoteReply reply = stub(m.to()).requestVote(ProtoCodec.toProto(m));
            onLoop(() -> deliver(ProtoCodec.fromProto(reply, id)));
        } catch (Exception e) {
            // An unreachable peer is normal, not exceptional. Raft retries on the next
            // tick, so there is nothing to do but let this attempt go.
            log.log(Level.DEBUG, "RequestVote to %s failed: %s".formatted(m.to(), e.getMessage()));
        }
    }

    private void sendAppendEntries(Message.AppendEntries m) {
        try {
            AppendEntriesReply reply = stub(m.to()).appendEntries(ProtoCodec.toProto(m));
            onLoop(() -> deliver(ProtoCodec.fromProto(reply, id)));
        } catch (Exception e) {
            log.log(Level.DEBUG, "AppendEntries to %s failed: %s".formatted(m.to(), e.getMessage()));
        }
    }

    private void sendInstallSnapshot(Message.InstallSnapshot m) {
        try {
            InstallSnapshotReply reply = stub(m.to()).installSnapshot(ProtoCodec.toProto(m));
            onLoop(() -> deliver(ProtoCodec.fromProto(reply, id)));
        } catch (Exception e) {
            log.log(Level.DEBUG, "InstallSnapshot to %s failed: %s".formatted(m.to(), e.getMessage()));
        }
    }

    /** Feeds a message into the node. Must be called on the loop. */
    private void deliver(Message message) {
        node.receive(message);
        applyAndDispatch();
    }

    private RaftGrpc.RaftBlockingStub stub(NodeId peer) {
        RaftGrpc.RaftBlockingStub stub = stubs.get(peer);
        if (stub == null) {
            throw new IllegalStateException("no channel to " + peer);
        }
        return stub;
    }

    // --- client operations --------------------------------------------------------------

    /**
     * Proposes a command and waits for it to be applied.
     *
     * @return the read result for a read command, empty for a write
     * @throws NotLeaderException if this node cannot accept the proposal
     */
    CompletableFuture<Optional<String>> propose(byte[] command) {
        CompletableFuture<Optional<String>> result = new CompletableFuture<>();

        onLoop(
                () -> {
                    Optional<Long> index = node.propose(command);
                    if (index.isEmpty()) {
                        result.completeExceptionally(new NotLeaderException(leaderHint()));
                        return;
                    }
                    pending.put(index.get(), new PendingRequest(command, result));
                    applyAndDispatch();
                });

        return result;
    }

    private String leaderHint() {
        return node.leaderId().map(NodeId::value).orElse("");
    }

    /** A snapshot of this node's state, for the Status RPC and for tests. */
    public Status status() {
        return onLoop(
                () ->
                        new Status(
                                id.value(),
                                node.role(),
                                node.currentTerm(),
                                node.commitIndex(),
                                leaderHint(),
                                node.configuration().members().stream().map(NodeId::value).sorted().toList()));
    }

    public record Status(
            String nodeId,
            Role role,
            long term,
            long commitIndex,
            String leaderId,
            List<String> members) {}

    // --- gRPC services ------------------------------------------------------------------

    /**
     * The peer-facing service.
     *
     * <p>Each handler feeds the request into the node on the loop thread and returns the
     * response the node produced. The node answers synchronously, so the reply is always
     * waiting in its outbound queue when {@code receive} returns.
     */
    private final class RaftService extends RaftGrpc.RaftImplBase {

        @Override
        public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteReply> observer) {
            respond(
                    observer,
                    () ->
                            replyTo(
                                    ProtoCodec.fromProto(request, id),
                                    Message.RequestVoteResponse.class,
                                    ProtoCodec::toProto,
                                    // A vote request may be discarded outright by the §4.2.3
                                    // guard, in which case there is no reply to send. An
                                    // empty response is a refusal the caller can act on.
                                    RequestVoteReply.newBuilder()
                                            .setFrom(id.value())
                                            .setVoteGranted(false)
                                            .build()));
        }

        @Override
        public void appendEntries(
                AppendEntriesRequest request, StreamObserver<AppendEntriesReply> observer) {
            respond(
                    observer,
                    () ->
                            replyTo(
                                    ProtoCodec.fromProto(request, id),
                                    Message.AppendEntriesResponse.class,
                                    ProtoCodec::toProto,
                                    AppendEntriesReply.newBuilder()
                                            .setFrom(id.value())
                                            .setSuccess(false)
                                            .build()));
        }

        @Override
        public void installSnapshot(
                InstallSnapshotRequest request, StreamObserver<InstallSnapshotReply> observer) {
            respond(
                    observer,
                    () ->
                            replyTo(
                                    ProtoCodec.fromProto(request, id),
                                    Message.InstallSnapshotResponse.class,
                                    ProtoCodec::toProto,
                                    InstallSnapshotReply.newBuilder().setFrom(id.value()).build()));
        }
    }

    /**
     * Delivers a request to the node and extracts the reply it generated.
     *
     * <p>Anything else the node queued — heartbeats to other peers, say — is dispatched
     * normally rather than discarded.
     */
    private <M extends Message, P> P replyTo(
            Message request,
            Class<M> responseType,
            java.util.function.Function<M, P> encode,
            P fallback) {

        return onLoop(
                () -> {
                    node.receive(request);

                    P encoded = null;
                    List<Message> outbound = node.drainOutbound();
                    List<Message> forwarding = new java.util.ArrayList<>(outbound.size());

                    for (Message message : outbound) {
                        if (encoded == null
                                && responseType.isInstance(message)
                                && message.to().equals(request.from())) {
                            encoded = encode.apply(responseType.cast(message));
                        } else {
                            forwarding.add(message);
                        }
                    }

                    node.takeSnapshotToRestore()
                            .ifPresent(
                                    snapshot ->
                                            stateMachine.restore(
                                                    snapshot.data(), snapshot.lastIncludedIndex()));
                    for (LogEntry entry : node.drainCommitted()) {
                        stateMachine.apply(entry);
                    }
                    dispatch(forwarding);

                    return encoded == null ? fallback : encoded;
                });
    }

    private static <T> void respond(StreamObserver<T> observer, java.util.function.Supplier<T> work) {
        try {
            observer.onNext(work.get());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    KeyValueStore stateMachine() {
        return stateMachine;
    }

    RaftNode node() {
        return node;
    }

    @Override
    public void close() {
        running = false;
        ticker.shutdownNow();
        senders.shutdownNow();
        loop.shutdownNow();

        channels.values().forEach(ManagedChannel::shutdownNow);
        grpcServer.shutdownNow();

        try {
            grpcServer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
