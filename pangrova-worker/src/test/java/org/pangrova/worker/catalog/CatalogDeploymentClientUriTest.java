package org.pangrova.worker.catalog;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogDeploymentClientUriTest {
    @Test
    void preservesOneCatalogPathSegmentAndSeparateIncarnationQueryValue() throws Exception {
        var received = new AtomicReference<URI>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                received.set(exchange.getRequestURI());
                exchange.sendResponseHeaders(404, -1);
            }
        });
        server.start();
        try {
            var client = new CatalogDeploymentClient("http://127.0.0.1:" + server.getAddress().getPort(), "secret");
            assertThatThrownBy(() -> client.fetch("a b/+猫", "i &j#2", 9))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("404");
            assertThat(received.get().getRawPath())
                    .isEqualTo("/v1/internal/catalog-deployments/a%20b%2F%2B%E7%8C%AB");
            assertThat(received.get().getRawQuery()).isEqualTo("incarnation=i%20%26j%232&revision=9");
        } finally {
            server.stop(0);
        }
    }
}
