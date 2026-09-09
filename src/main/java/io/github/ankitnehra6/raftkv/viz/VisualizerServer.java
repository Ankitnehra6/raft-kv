package io.github.ankitnehra6.raftkv.viz;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.ankitnehra6.raftkv.core.LogEntry;
import io.github.ankitnehra6.raftkv.core.Message;
import io.github.ankitnehra6.raftkv.core.NodeId;
import io.github.ankitnehra6.raftkv.core.RaftNode;
import io.github.ankitnehra6.raftkv.sim.SimulatedCluster;
import io.github.ankitnehra6.raftkv.sim.SimulatedNetwork;
import io.github.ankitnehra6.raftkv.statemachine.Command;
import io.github.ankitnehra6.raftkv.statemachine.KeyValueStore;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Serves an interactive visualisation of a live cluster.
 *
 * <p>The important thing about it is what it is <em>not</em>: a re-implementation of Raft in
 * JavaScript. Every node on screen is a real {@link RaftNode} — the same class the test
 * suite hammers and the same one {@code RaftServer} runs behind gRPC. Clicking "crash" calls
 * the same method a test does. A diagram that animated a separate model would be a drawing
 * of Raft; this is the algorithm itself, rendered.
 *
 * <p>Uses the JDK's own HTTP server, so the visualiser adds no dependency.
 *
 * <p>Single-threaded on purpose: {@link RaftNode} is not thread-safe, and every request is
 * serialised on one lock rather than the server being made concurrent for a demo that has
 * one user.
 */
public class VisualizerServer implements AutoCloseable {

    /** Events kept for the on-screen log. Older ones scroll away. */
    private static final int EVENT_HISTORY = 60;

    private final HttpServer http;
    private final Object lock = new Object();

    private SimulatedCluster cluster;
    private Map<NodeId, KeyValueStore> stores = new LinkedHashMap<>();
    private Map<NodeId, Integer> consumed = new LinkedHashMap<>();
    private final Deque<String> events = new ArrayDeque<>();

    private int size = 5;
    private long seed = 42;

    public VisualizerServer(int port) throws IOException {
        this.http = HttpServer.create(new InetSocketAddress(port), 0);

        http.createContext("/api/state", this::handleState);
        http.createContext("/api/step", this::handleStep);
        http.createContext("/api/propose", this::handlePropose);
        http.createContext("/api/crash", this::handleCrash);
        http.createContext("/api/restart", this::handleRestart);
        http.createContext("/api/partition", this::handlePartition);
        http.createContext("/api/heal", this::handleHeal);
        http.createContext("/api/network", this::handleNetwork);
        http.createContext("/api/reset", this::handleReset);
        http.createContext("/", this::handleStatic);

        // Requests mutate one cluster, so concurrency would only create races.
        http.setExecutor(java.util.concurrent.Executors.newSingleThreadExecutor());

        reset(size, seed);
    }

    public void start() {
        http.start();
        System.out.printf("Raft visualiser on http://localhost:%d%n", http.getAddress().getPort());
    }

    public int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
    }

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        VisualizerServer server = new VisualizerServer(port);
        server.start();
    }

    // --- cluster lifecycle -----------------------------------------------------------

    private void reset(int newSize, long newSeed) {
        size = Math.clamp(newSize, 1, 9);
        seed = newSeed;
        cluster = new SimulatedCluster(size, seed);
        stores = new LinkedHashMap<>();
        consumed = new LinkedHashMap<>();
        cluster.nodes()
                .forEach(
                        n -> {
                            stores.put(n.id(), new KeyValueStore());
                            consumed.put(n.id(), 0);
                        });
        events.clear();
        record("cluster of %d created, seed %d".formatted(size, seed));
    }

    /** Advances the cluster and keeps each node's state machine in step. */
    private void step(int ticks) {
        for (int i = 0; i < ticks; i++) {
            Map<NodeId, String> rolesBefore = roleSnapshot();
            cluster.tick();
            applyCommitted();
            noteRoleChanges(rolesBefore);
        }
    }

    private Map<NodeId, String> roleSnapshot() {
        return cluster.nodes().stream()
                .collect(
                        Collectors.toMap(
                                RaftNode::id,
                                n -> n.role().name() + "@" + n.currentTerm(),
                                (a, b) -> a,
                                LinkedHashMap::new));
    }

    /** Turns role transitions into readable events, which is most of what a viewer wants. */
    private void noteRoleChanges(Map<NodeId, String> before) {
        for (RaftNode node : cluster.nodes()) {
            String was = before.get(node.id());
            String now = node.role().name() + "@" + node.currentTerm();
            if (was != null && !was.equals(now)) {
                record("%s %s → %s".formatted(node.id(), was, now));
            }
        }
    }

    private void applyCommitted() {
        for (RaftNode node : cluster.nodes()) {
            stores.computeIfAbsent(node.id(), unused -> new KeyValueStore());
            consumed.putIfAbsent(node.id(), 0);

            node.takeSnapshotToRestore()
                    .ifPresent(
                            snapshot -> {
                                stores.get(node.id())
                                        .restore(snapshot.data(), snapshot.lastIncludedIndex());
                                consumed.put(node.id(), cluster.appliedAt(node.id()).size());
                            });

            List<LogEntry> applied = cluster.appliedAt(node.id());
            int already = consumed.get(node.id());
            if (applied.size() > already) {
                consumed.put(node.id(), applied.size());
                applied.subList(already, applied.size())
                        .forEach(entry -> stores.get(node.id()).apply(entry));
            }
        }
    }

    private void record(String event) {
        events.addFirst("t%d  %s".formatted(cluster == null ? 0 : cluster.currentTick(), event));
        while (events.size() > EVENT_HISTORY) {
            events.removeLast();
        }
    }

    // --- handlers ---------------------------------------------------------------------

    private void handleState(HttpExchange exchange) throws IOException {
        synchronized (lock) {
            respondJson(exchange, buildState());
        }
    }

    private void handleStep(HttpExchange exchange) throws IOException {
        synchronized (lock) {
            step(intParam(exchange, "ticks", 1));
            respondJson(exchange, buildState());
        }
    }

    private void handlePropose(HttpExchange exchange) throws IOException {
        synchronized (lock) {
            String key = param(exchange, "key").orElse("k");
            String value = param(exchange, "value").orElse("v");

            Optional<RaftNode> leader = cluster.leader();
            if (leader.isEmpty()) {
                record("proposal rejected: no leader");
            } else {
                leader.get().propose(Command.encode(new Command.Put(key, value)));
                record("%s proposed %s=%s".formatted(leader.get().id(), key, value));
            }
            respondJson(exchange, buildState());
        }
    }

    private void handleCrash(HttpExchange exchange) throws IOException {
        synchronized (lock) {
            param(exchange, "node")
                    .ifPresent(
                            node -> {
                                cluster.crash(node);
                                record(node + " crashed");
                            });
            respondJson(exchange, buildState());
        }
    }

    private void handleRestart(HttpExchange exchange) throws IOException {
        synchronized (lock) {
            param(exchange, "node")
                    .ifPresent(
                            node -> {
                                cluster.restart(node);
                                record(node + " restarted (recovered term, vote and log)");
                            });
            respondJson(exchange, buildState());
        }
    }

    private void handlePartition(HttpExchange exchange) throws IOException {
        synchronized (lock) {
            // "n0,n1" on one side, everyone else on the other.
            List<String> group =
                    param(exchange, "group").map(g -> List.of(g.split(","))).orElse(List.of());

            if (group.isEmpty()) {
                respondJson(exchange, buildState());
                return;
            }
            List<String> rest =
                    cluster.nodes().stream()
                            .map(n -> n.id().value())
                            .filter(name -> !group.contains(name))
                            .toList();

            if (rest.isEmpty()) {
                respondJson(exchange, buildState());
                return;
            }
            cluster.partition(group, rest);
            record("partitioned %s | %s".formatted(group, rest));
            respondJson(exchange, buildState());
        }
    }

    private void handleHeal(HttpExchange exchange) throws IOException {
        synchronized (lock) {
            cluster.heal();
            record("network healed");
            respondJson(exchange, buildState());
        }
    }

    private void handleNetwork(HttpExchange exchange) throws IOException {
        synchronized (lock) {
            param(exchange, "drop")
                    .ifPresent(
                            drop -> {
                                double rate = Double.parseDouble(drop);
                                cluster.setDropRate(rate);
                                record("packet loss set to %.0f%%".formatted(rate * 100));
                            });
            param(exchange, "maxDelay")
                    .ifPresent(
                            delay -> {
                                int max = Integer.parseInt(delay);
                                cluster.setDelay(1, Math.max(1, max));
                                record("delay set to 1-%d ticks".formatted(max));
                            });
            respondJson(exchange, buildState());
        }
    }

    private void handleReset(HttpExchange exchange) throws IOException {
        synchronized (lock) {
            reset(intParam(exchange, "size", size), longParam(exchange, "seed", System.nanoTime()));
            respondJson(exchange, buildState());
        }
    }

    // --- state serialisation ------------------------------------------------------------

    private String buildState() {
        Map<String, Object> state = Json.map();
        state.put("tick", cluster.currentTick());
        state.put("size", size);
        state.put("seed", seed);

        long maxLogIndex =
                cluster.nodes().stream().mapToLong(n -> n.log().lastIndex()).max().orElse(0);
        state.put("maxLogIndex", maxLogIndex);

        List<Object> nodes = Json.list();
        for (RaftNode node : cluster.nodes()) {
            Map<String, Object> n = Json.map();
            String id = node.id().value();

            n.put("id", id);
            n.put("role", node.role().name());
            n.put("term", node.currentTerm());
            n.put("commitIndex", node.commitIndex());
            n.put("lastIndex", node.log().lastIndex());
            n.put("snapshotIndex", node.log().snapshotIndex());
            n.put("crashed", cluster.isCrashed(id));
            n.put("partitionGroup", cluster.partitionGroupOf(id));
            n.put("leaderId", node.leaderId().map(NodeId::value).orElse(""));
            n.put("votedFor", node.votedFor().map(NodeId::value).orElse(""));
            n.put("members", node.configuration().members().stream().map(NodeId::value).sorted().toList());
            n.put("stateSize", stores.get(node.id()).size());

            List<Object> entries = Json.list();
            for (LogEntry entry : node.log().entries()) {
                Map<String, Object> e = Json.map();
                e.put("index", entry.index());
                e.put("term", entry.term());
                e.put("type", entry.type().name());
                e.put("committed", entry.index() <= node.commitIndex());
                entries.add(e);
            }
            n.put("log", entries);
            nodes.add(n);
        }
        state.put("nodes", nodes);

        List<Object> messages = Json.list();
        for (SimulatedNetwork.InFlightView view : cluster.inFlight()) {
            Message message = view.message();
            Map<String, Object> m = Json.map();
            m.put("from", message.from().value());
            m.put("to", message.to().value());
            m.put("kind", kindOf(message));
            m.put("term", message.term());
            m.put("progress", Math.clamp(view.progress(), 0.0, 1.0));
            messages.add(m);
        }
        state.put("messages", messages);

        state.put("events", new ArrayList<Object>(events));
        return Json.object(state);
    }

    /** A short label for the wire, so the UI does not have to know the class hierarchy. */
    private static String kindOf(Message message) {
        return switch (message) {
            case Message.RequestVote ignored -> "vote-request";
            case Message.RequestVoteResponse m -> m.voteGranted() ? "vote-granted" : "vote-denied";
            case Message.AppendEntries m -> m.isHeartbeat() ? "heartbeat" : "append";
            case Message.AppendEntriesResponse m -> m.success() ? "append-ok" : "append-reject";
            case Message.InstallSnapshot ignored -> "snapshot";
            case Message.InstallSnapshotResponse ignored -> "snapshot-ok";
        };
    }

    // --- plumbing -------------------------------------------------------------------------

    private static Optional<String> param(HttpExchange exchange, String name) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return Optional.empty();
        }
        for (String pair : query.split("&")) {
            int equals = pair.indexOf('=');
            if (equals > 0 && pair.substring(0, equals).equals(name)) {
                return Optional.of(URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8));
            }
        }
        return Optional.empty();
    }

    private static int intParam(HttpExchange exchange, String name, int fallback) {
        return param(exchange, name).map(Integer::parseInt).orElse(fallback);
    }

    private static long longParam(HttpExchange exchange, String name, long fallback) {
        return param(exchange, name).map(Long::parseLong).orElse(fallback);
    }

    private static void respondJson(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.getResponseHeaders().add("Cache-Control", "no-store");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private void handleStatic(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/") || path.isEmpty()) {
            path = "/index.html";
        }

        // Only ever serves from the packaged resource directory, and a path containing ".."
        // is rejected rather than normalised — a traversal here would read anything on the
        // classpath.
        if (path.contains("..")) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        String resource = "/viz" + path;
        try (InputStream in = VisualizerServer.class.getResourceAsStream(resource)) {
            if (in == null) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
                return;
            }
            byte[] bytes = in.readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", contentTypeOf(path));
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }
    }

    private static String contentTypeOf(String path) {
        if (path.endsWith(".html")) {
            return "text/html; charset=utf-8";
        }
        if (path.endsWith(".css")) {
            return "text/css; charset=utf-8";
        }
        if (path.endsWith(".js")) {
            return "application/javascript; charset=utf-8";
        }
        return "application/octet-stream";
    }
}
