package io.github.ankitnehra6.raftkv.viz;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The visualiser's HTTP surface.
 *
 * <p>Thin on purpose. The behaviour it displays is tested exhaustively elsewhere; what
 * matters here is that the endpoints exist, that the JSON is well-formed, and that the
 * controls actually reach the cluster rather than only appearing to.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class VisualizerServerTest {

    private VisualizerServer server;
    private HttpClient client;
    private String base;

    @BeforeEach
    void start() throws IOException {
        server = new VisualizerServer(0); // any free port
        server.start();
        client = HttpClient.newHttpClient();
        base = "http://localhost:" + server.port();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create(base + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void servesThePage() throws Exception {
        assertThat(get("/").statusCode()).isEqualTo(200);
        assertThat(get("/").body()).contains("Raft");
        assertThat(get("/style.css").statusCode()).isEqualTo(200);
        assertThat(get("/app.js").statusCode()).isEqualTo(200);
    }

    @Test
    void reportsClusterState() throws Exception {
        String body = get("/api/state").body();

        assertThat(body).contains("\"tick\"", "\"nodes\"", "\"messages\"", "\"events\"");
        assertThat(body).contains("\"role\"", "\"term\"", "\"commitIndex\"", "\"log\"");
    }

    /** Stepping must actually advance the cluster, not just return a fresh snapshot. */
    @Test
    void steppingElectsALeader() throws Exception {
        get("/api/step?ticks=60");
        String body = get("/api/state").body();

        assertThat(body).contains("\"role\":\"LEADER\"");
    }

    @Test
    void proposingAppendsToTheLog() throws Exception {
        get("/api/step?ticks=60");
        get("/api/propose?key=city&value=bengaluru");
        get("/api/step?ticks=40");

        // The entry must show up as committed somewhere.
        assertThat(get("/api/state").body()).contains("\"committed\":true");
    }

    @Test
    void crashingAndRestartingAreReflected() throws Exception {
        get("/api/step?ticks=60");

        get("/api/crash?node=n0");
        assertThat(get("/api/state").body()).contains("\"id\":\"n0\",\"role\"");
        assertThat(get("/api/state").body()).contains("\"crashed\":true");

        get("/api/restart?node=n0");
        assertThat(get("/api/state").body()).doesNotContain("\"crashed\":true");
    }

    @Test
    void partitioningSplitsTheCluster() throws Exception {
        get("/api/step?ticks=60");
        get("/api/partition?group=n0,n1");

        // Two distinct partition groups must appear.
        String body = get("/api/state").body();
        assertThat(body).contains("\"partitionGroup\":0").contains("\"partitionGroup\":1");

        get("/api/heal");
        assertThat(get("/api/state").body()).doesNotContain("\"partitionGroup\":1");
    }

    @Test
    void resetBuildsANewCluster() throws Exception {
        get("/api/step?ticks=50");
        get("/api/reset?size=3&seed=7");

        String body = get("/api/state").body();
        assertThat(body).contains("\"size\":3").contains("\"tick\":0").contains("\"seed\":7");
    }

    /** A path traversal must be refused rather than normalised into a classpath read. */
    @Test
    void rejectsPathTraversal() throws Exception {
        assertThat(get("/../pom.xml").statusCode()).isNotEqualTo(200);
    }

    @Test
    void unknownAssetsAreNotFound() throws Exception {
        assertThat(get("/nope.js").statusCode()).isEqualTo(404);
    }

    /** Escaping matters: unescaped control characters would produce invalid JSON. */
    @Test
    void jsonEscapesAwkwardStrings() {
        assertThat(Json.quote("say \"hi\"")).isEqualTo("\"say \\\"hi\\\"\"");
        assertThat(Json.quote("line\nbreak")).isEqualTo("\"line\\nbreak\"");
        assertThat(Json.quote("back\\slash")).isEqualTo("\"back\\\\slash\"");
        assertThat(Json.quote("\u0001")).isEqualTo("\"\\u0001\"");
    }
}
