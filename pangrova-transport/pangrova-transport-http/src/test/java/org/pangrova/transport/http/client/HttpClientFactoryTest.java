package org.pangrova.transport.http.client;

import java.net.Proxy;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.eclipse.jetty.server.Server;
import org.pangrova.transport.http.EmbeddedHttpServerFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpClientFactoryTest {

    private Server server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        ServletContextHandler handler = new ServletContextHandler();
        handler.addServlet(new ServletHolder(new EchoServlet()), "/echo");
        server = EmbeddedHttpServerFactory.create(0, handler);
        server.start();
        port = EmbeddedHttpServerFactory.getPort(server);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void createDefaultReturnsNonNullClient() {
        ManagedHttpClient managed = HttpClientFactory.createDefault();
        assertThat(managed).isNotNull();
        assertThat(managed.httpClient()).isNotNull();
        managed.close();
    }

    @Test
    void internalClientAlwaysSelectsDirectConnectionForWorkerAddress() {
        try (ManagedHttpClient managed = HttpClientFactory.createDefault()) {
            assertThat(managed.httpClient().proxy()).isPresent();
            assertThat(managed.httpClient().proxy().orElseThrow()
                    .select(URI.create("http://192.168.1.15:59457/v1/task")))
                    .containsExactly(Proxy.NO_PROXY);
        }
    }

    @Test
    void createWithConfigRespectsConnectTimeout() {
        HttpClientConfig config = new HttpClientConfig("test", 32, Duration.ofSeconds(3), Duration.ofSeconds(10));
        ManagedHttpClient managed = HttpClientFactory.create(config);
        assertThat(managed.httpClient().connectTimeout()).isPresent();
        assertThat(managed.httpClient().connectTimeout().get()).isEqualTo(Duration.ofSeconds(3));
        managed.close();
    }

    @Test
    void defaultConfigHasExpectedValues() {
        HttpClientConfig config = HttpClientConfig.DEFAULT;
        assertThat(config.maxConnections()).isEqualTo(64);
        assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void configRejectsZeroMaxConnections() {
        assertThatThrownBy(() -> new HttpClientConfig("test", 0, Duration.ofSeconds(5), Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConnections");
    }

    @Test
    void configRejectsNegativeMaxConnections() {
        assertThatThrownBy(() -> new HttpClientConfig("test", -1, Duration.ofSeconds(5), Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConnections");
    }

    @Test
    void configRejectsNullConnectTimeout() {
        assertThatThrownBy(() -> new HttpClientConfig("test", 64, null, Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connectTimeout");
    }

    @Test
    void configRejectsNullRequestTimeout() {
        assertThatThrownBy(() -> new HttpClientConfig("test", 64, Duration.ofSeconds(5), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requestTimeout");
    }

    @Test
    void createdClientCanMakeHttpRequest() throws Exception {
        ManagedHttpClient managed = HttpClientFactory.createDefault();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/echo"))
                .GET()
                .build();

        HttpResponse<String> response = managed.httpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("OK");
        managed.close();
    }

    /** Minimal servlet returning "OK" for real HTTP round-trip testing. */
    static class EchoServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws java.io.IOException {
            resp.setStatus(200);
            resp.getWriter().write("OK");
        }
    }
}
