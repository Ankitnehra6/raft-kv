package io.github.ankitnehra6.raftkv.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ankitnehra6.raftkv.core.NodeId;
import io.github.ankitnehra6.raftkv.core.Role;
import io.github.ankitnehra6.raftkv.grpc.GetReply;
import io.github.ankitnehra6.raftkv.grpc.GetRequest;
import io.github.ankitnehra6.raftkv.grpc.KeyValueGrpc;
import io.github.ankitnehra6.raftkv.grpc.PutRequest;
import io.github.ankitnehra6.raftkv.grpc.StatusRequest;
import io.github.ankitnehra6.raftkv.grpc.WriteReply;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A real three-node cluster over real sockets.
 *
 * <p>The exhaustive correctness testing lives in the simulation, where faults are cheap and
 * runs are reproducible. This suite answers the one question simulation cannot: that the
 * same {@link io.github.ankitnehra6.raftkv.core.RaftNode} works when the driver supplying
 * its time and transport is a scheduler and a network rather than a loop.
 *
 * <p>It is deliberately small. Testing partition tolerance here would mean waiting on real
 * timeouts for behaviour already proven deterministically, which is slower and less certain.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RaftServerIntegrationTest {

    private final List<RaftServer> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();

    @BeforeEach
    void startCluster() throws IOException {
        // Ports are reserved by binding and immediately releasing. A server cannot be
        // constructed without knowing its peers' addresses, so they must be chosen first.
        Map<NodeId, Integer> ports = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                ports.put(NodeId.of("n" + i), socket.getLocalPort());
            }
        }

        for (Map.Entry<NodeId, Integer> entry : ports.entrySet()) {
            Map<NodeId, String> peers = new HashMap<>();
            ports.forEach(
                    (peerId, port) -> {
                        if (!peerId.equals(entry.getKey())) {
                            peers.put(peerId, "localhost:" + port);
                        }
                    });

            RaftServer server = new RaftServer(entry.getKey(), peers, entry.getValue());
            server.start();
            servers.add(server);
        }
    }

    @AfterEach
    void stopCluster() {
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(RaftServer::close);
    }

    /** Waits for a condition, polling. Real time here, unlike the simulation. */
    private static boolean await(Duration limit, java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
    }

    private Optional<RaftServer> leader() {
        List<RaftServer> leaders =
                servers.stream().filter(s -> s.status().role() == Role.LEADER).toList();
        return leaders.size() == 1 ? Optional.of(leaders.getFirst()) : Optional.empty();
    }

    private KeyValueGrpc.KeyValueBlockingStub clientFor(RaftServer server) {
        ManagedChannel channel =
                NettyChannelBuilder.forTarget("localhost:" + server.port()).usePlaintext().build();
        channels.add(channel);
        return KeyValueGrpc.newBlockingStub(channel);
    }

    @Test
    void electsALeaderOverRealSockets() {
        assertThat(await(Duration.ofSeconds(15), () -> leader().isPresent()))
                .as("three real nodes must elect exactly one leader")
                .isTrue();

        RaftServer leader = leader().orElseThrow();
        assertThat(leader.status().term()).isPositive();

        // Everyone must agree on who leads.
        assertThat(
                        await(
                                Duration.ofSeconds(10),
                                () ->
                                        servers.stream()
                                                .allMatch(
                                                        s ->
                                                                s.status()
                                                                        .leaderId()
                                                                        .equals(leader.id().value()))))
                .as("every node should recognise the same leader")
                .isTrue();
    }

    @Test
    void writesAndReadsThroughTheApi() {
        assertThat(await(Duration.ofSeconds(15), () -> leader().isPresent())).isTrue();

        KeyValueGrpc.KeyValueBlockingStub client = clientFor(leader().orElseThrow());

        WriteReply written =
                client.put(PutRequest.newBuilder().setKey("city").setValue("bengaluru").build());
        assertThat(written.getOk()).as("the leader must accept a write").isTrue();

        GetReply read = client.get(GetRequest.newBuilder().setKey("city").build());
        assertThat(read.getOk()).isTrue();
        assertThat(read.getFound()).isTrue();
        assertThat(read.getValue()).isEqualTo("bengaluru");
    }

    @Test
    void readsSeeTheLatestWrite() {
        assertThat(await(Duration.ofSeconds(15), () -> leader().isPresent())).isTrue();
        KeyValueGrpc.KeyValueBlockingStub client = clientFor(leader().orElseThrow());

        client.put(PutRequest.newBuilder().setKey("k").setValue("first").build());
        client.put(PutRequest.newBuilder().setKey("k").setValue("second").build());

        assertThat(client.get(GetRequest.newBuilder().setKey("k").build()).getValue())
                .isEqualTo("second");
    }

    @Test
    void anAbsentKeyIsReportedAsNotFound() {
        assertThat(await(Duration.ofSeconds(15), () -> leader().isPresent())).isTrue();
        KeyValueGrpc.KeyValueBlockingStub client = clientFor(leader().orElseThrow());

        GetReply reply = client.get(GetRequest.newBuilder().setKey("never-written").build());

        assertThat(reply.getOk()).isTrue();
        assertThat(reply.getFound()).isFalse();
    }

    /**
     * A write must reach every replica, not just the one that accepted it. Checked through
     * each node's own state machine rather than through the API, so it cannot be satisfied
     * by a request being forwarded.
     */
    @Test
    void writesReplicateToEveryNode() {
        assertThat(await(Duration.ofSeconds(15), () -> leader().isPresent())).isTrue();
        KeyValueGrpc.KeyValueBlockingStub client = clientFor(leader().orElseThrow());

        client.put(PutRequest.newBuilder().setKey("replicated").setValue("yes").build());

        assertThat(
                        await(
                                Duration.ofSeconds(10),
                                () ->
                                        servers.stream()
                                                .allMatch(
                                                        s ->
                                                                s.stateMachine()
                                                                        .get("replicated")
                                                                        .filter("yes"::equals)
                                                                        .isPresent())))
                .as("every replica must hold the write")
                .isTrue();
    }

    /**
     * A non-leader answers with a hint rather than an error. Losing leadership is routine,
     * and a client that must parse an exception to find the leader will get it wrong.
     */
    @Test
    void aFollowerRedirectsRatherThanFailing() {
        assertThat(await(Duration.ofSeconds(15), () -> leader().isPresent())).isTrue();
        RaftServer leader = leader().orElseThrow();

        RaftServer follower =
                servers.stream().filter(s -> !s.equals(leader)).findFirst().orElseThrow();

        WriteReply reply =
                clientFor(follower)
                        .put(PutRequest.newBuilder().setKey("k").setValue("v").build());

        assertThat(reply.getOk()).isFalse();
        assertThat(reply.getLeaderHint())
                .as("the follower knows who the leader is and must say so")
                .isEqualTo(leader.id().value());
    }

    @Test
    void statusReportsTheCluster() {
        assertThat(await(Duration.ofSeconds(15), () -> leader().isPresent())).isTrue();

        var status = clientFor(leader().orElseThrow()).status(StatusRequest.getDefaultInstance());

        assertThat(status.getRole()).isEqualTo("LEADER");
        assertThat(status.getTerm()).isPositive();
        assertThat(status.getMembersList()).containsExactlyInAnyOrder("n0", "n1", "n2");
    }

    /** The cluster must survive losing a minority and keep accepting writes. */
    @Test
    void survivesLosingAFollower() {
        assertThat(await(Duration.ofSeconds(15), () -> leader().isPresent())).isTrue();
        RaftServer leader = leader().orElseThrow();

        RaftServer victim =
                servers.stream().filter(s -> !s.equals(leader)).findFirst().orElseThrow();
        victim.close();
        servers.remove(victim);

        KeyValueGrpc.KeyValueBlockingStub client = clientFor(leader);
        WriteReply reply =
                client.put(PutRequest.newBuilder().setKey("after-loss").setValue("ok").build());

        assertThat(reply.getOk()).as("two of three is still a majority").isTrue();
        assertThat(client.get(GetRequest.newBuilder().setKey("after-loss").build()).getValue())
                .isEqualTo("ok");
    }
}
