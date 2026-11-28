package org.pangrova.server.coordinator.bootstrap;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.connector.api.plugin.ConnectorFactory;
import org.pangrova.coordinator.metadata.CatalogExecutionBindingFactory;
import org.pangrova.coordinator.metadata.PluginConnectorHandleFactory;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;
import org.pangrova.worker.catalog.WorkerCatalogRuntime;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExternalCatalogRolesIntegrationTest {
    @TempDir Path root;

    @Test
    void rolesExecuteTheirOwnPluginAndDrainCatalogLeasesBeforeClosingEachLoader() throws Exception {
        var coordinatorPlugins = writePlugin("coordinator");
        var workerPlugins = writePlugin("worker");
        var configuration = org.pangrova.config.PangrovaConfiguration.fromYaml(
                Map.of("connector.plugin-directory", coordinatorPlugins.toString()));
        var configuredRoot = PangrovaCoordinatorMain.buildBootstrapConfig(configuration).core().pluginDirectory();
        var events = root.resolve("events");
        var descriptor = new CatalogDescriptor("external", CatalogType.CONNECTOR, "external-test",
                CatalogState.ACTIVE, Map.of("events", events.toString()), "", Set.of());
        var deployment = new CatalogDeploymentResponse(descriptor.name(), descriptor.provider(),
                descriptor.incarnationId(), descriptor.revision(), descriptor.properties(), Set.of());
        byte[] payload = JsonMapperFactory.create().writeValueAsBytes(deployment);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/internal/catalog-deployments/external", exchange -> {
            try (exchange) {
                if (!"Bearer role-test-secret".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                    exchange.sendResponseHeaders(403, -1);
                    return;
                }
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
            }
        });
        server.start();
        ClassLoader host = getClass().getClassLoader();
        try {
            var control = new PluginConnectorHandleFactory(host, new SecretResolverRegistry(),
                    unused -> Optional.empty(), unused -> Optional.empty(), configuredRoot);
            try (var runtime = WorkerCatalogRuntime.discover("http://127.0.0.1:" + server.getAddress().getPort(),
                    "role-test-secret", host, Optional.of(workerPlugins))) {
                var handle = control.create(descriptor);
                var binding = CatalogExecutionBindingFactory.from(descriptor);
                var security = SecurityContext.newBuilder().setPrincipal("user").setTenantId("tenant").build();
                try (var lease = runtime.leaseControl(binding, security, Optional.empty())) {
                    assertThatThrownBy(runtime::close).hasMessageContaining("active leases");
                    assertThatThrownBy(() -> runtime.leaseControl(binding, security, Optional.empty()))
                            .hasMessageContaining("closed");
                    assertThat(handle.checkHealth().isHealthy()).isTrue();
                    handle.shutdown();
                    control.shutdown();
                    assertThat(Files.readAllLines(events)).containsExactly("coordinator");
                }
                runtime.close();
                assertThat(Files.readAllLines(events)).containsExactly("coordinator", "worker");
            } finally {
                control.shutdown();
            }
        } finally {
            server.stop(0);
        }
    }

    private Path writePlugin(String role) throws IOException {
        var plugins = Files.createDirectory(root.resolve(role));
        var directory = Files.createDirectory(plugins.resolve("external"));
        try (var jar = new JarOutputStream(Files.newOutputStream(directory.resolve("external.jar")))) {
            jar.putNextEntry(new JarEntry("META-INF/services/" + ConnectorFactory.class.getName()));
            jar.write(ExternalCatalogTestFactory.class.getName().getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
            jar.putNextEntry(new JarEntry("external-catalog-marker"));
            jar.write((role + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
            writeClass(jar, ExternalCatalogTestFactory.class);
            writeClass(jar, ExternalCatalogTestFactory.ExternalCatalog.class);
        }
        try (var dependency = new JarOutputStream(Files.newOutputStream(directory.resolve("codec.jar")))) {
            writeClass(dependency, ExternalCatalogTestFactory.ExternalCodec.class);
        }
        return plugins;
    }

    private void writeClass(JarOutputStream jar, Class<?> type) throws IOException {
        String name = type.getName().replace('.', '/') + ".class";
        jar.putNextEntry(new JarEntry(name));
        try (var input = java.util.Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(name))) {
            input.transferTo(jar);
        }
        jar.closeEntry();
    }
}
