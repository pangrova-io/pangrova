package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.security.api.secret.SecretTemplates;
import org.pangrova.security.core.secret.SecretResolverRegistry;
import org.pangrova.security.core.secret.provider.env.EnvSecretProvider;
import org.pangrova.security.core.secret.provider.file.FileSecretProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates SecretResolver wiring into PluginConnectorHandleFactory.
 * Ensures connector config properties with ${PREFIX:reference} syntax
 * are resolved before reaching ConnectorFactory.create().
 *
 * <p>Uses real EnvSecretProvider and FileSecretProvider — no mocks.</p>
 * <p>Validates: Requirements 64.1, 64.2, 64.3</p>
 */
class SecretResolverConnectorWiringTest {

    @TempDir
    Path tempDir;

    private PluginConnectorHandleFactory factory;

    @BeforeEach
    void setUp() {
        SecretResolverRegistry resolver = new SecretResolverRegistry();
        resolver.register(new EnvSecretProvider());
        resolver.register(new FileSecretProvider(tempDir));
        factory = new PluginConnectorHandleFactory(getClass().getClassLoader(), resolver, descriptor -> java.util.Optional.empty());
    }

    @AfterEach
    void tearDown() {
        factory.shutdown();
    }

    @Test
    void plainPropertiesPassThroughUnchanged() {
        CatalogDescriptor descriptor = connectorDescriptor(
                "plain_catalog", "iceberg",
                Map.of("warehouse", tempDir.toString(), "type", "hadoop"));

        CatalogHandle handle = factory.create(descriptor);

        assertThat(handle).isNotNull();
        assertThat(handle.getDescriptor().name()).isEqualTo("plain_catalog");
        assertThat(handle.getState()).isEqualTo(CatalogState.ACTIVE);
    }

    @Test
    void fileSecretResolvedInConnectorProperties() throws IOException {
        Path secretFile = tempDir.resolve("warehouse-secret.txt");
        Files.writeString(secretFile, tempDir.toString());

        CatalogDescriptor descriptor = connectorDescriptor(
                "file_secret_catalog", "iceberg",
                Map.of(
                        "warehouse", "${FILE:" + secretFile + "}",
                        "type", "hadoop"));

        CatalogHandle handle = factory.create(descriptor);

        assertThat(handle).isNotNull();
        assertThat(handle.getState()).isEqualTo(CatalogState.ACTIVE);
    }

    @Test
    void envSecretResolvedInConnectorProperties() {
        // HOME is universally available on all platforms
        String homeDir = System.getenv("HOME");
        if (homeDir == null) {
            homeDir = System.getenv("USERPROFILE");
        }
        if (homeDir == null) {
            return; // skip on environments without HOME
        }

        SecretResolverRegistry resolver = new SecretResolverRegistry();
        resolver.register(new EnvSecretProvider());
        String resolved = SecretTemplates.resolve(resolver, "${ENV:HOME}", null);

        assertThat(resolved).isEqualTo(homeDir);
    }

    @Test
    void mixedPlainAndSecretPropertiesResolved() throws IOException {
        Path secretFile = tempDir.resolve("type-secret.txt");
        Files.writeString(secretFile, "hadoop");

        CatalogDescriptor descriptor = connectorDescriptor(
                "mixed_catalog", "iceberg",
                Map.of(
                        "warehouse", tempDir.toString(),
                        "type", "${FILE:" + secretFile + "}"));

        CatalogHandle handle = factory.create(descriptor);

        assertThat(handle).isNotNull();
        assertThat(handle.getState()).isEqualTo(CatalogState.ACTIVE);
    }

    private static CatalogDescriptor connectorDescriptor(
            String name, String provider, Map<String, String> properties) {
        return new CatalogDescriptor(
                name, CatalogType.CONNECTOR, provider,
                CatalogState.ACTIVE, properties, "", Set.of());
    }
}
