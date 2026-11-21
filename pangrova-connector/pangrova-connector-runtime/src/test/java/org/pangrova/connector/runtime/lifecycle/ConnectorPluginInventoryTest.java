package org.pangrova.connector.runtime.lifecycle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.connector.api.enums.ConnectorRole;
import org.pangrova.connector.api.plugin.ConnectorFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorPluginInventoryTest {
    @TempDir Path root;
    private final ClassLoader parent = getClass().getClassLoader();

    @Test
    void externalFactoryExecutesWithItsOwnResourcesAndRestoresTheCallingThread() throws Exception {
        writePluginJar("external", true);
        ClassLoader caller = Thread.currentThread().getContextClassLoader();
        try (var plugins = open(ConnectorRole.COORDINATOR)) {
            var factory = plugins.registry().require("test-plugin");
            factory.validateConfiguration(Map.of());
            assertThatThrownBy(() -> factory.create("catalog", Map.of(), null))
                    .isInstanceOf(IllegalArgumentException.class).hasMessage("plugin-local-catalog");
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(caller);
        }
        try (var worker = open(ConnectorRole.WORKER)) {
            assertThat(worker.registry().names()).doesNotContain("test-plugin");
        }
    }

    @Test
    void shutdownReleasesJarResourcesAndClosesAdmission() throws Exception {
        writePluginJar("external", true);
        var plugins = open(ConnectorRole.COORDINATOR);
        var factory = plugins.registry().require("test-plugin");
        factory.validateConfiguration(Map.of());
        plugins.close();
        plugins.close();
        assertThatThrownBy(plugins::registry).hasMessageContaining("closed");
        assertThatThrownBy(() -> factory.validateConfiguration(Map.of())).hasMessageContaining("plugin resource");
    }

    @Test
    void duplicateNamesAbortStartupAndARepairedDirectoryCanOpen() throws Exception {
        writePluginJar("a", true);
        var other = writePluginJar("b", true);
        var loaders = new java.util.ArrayList<org.pangrova.connector.runtime.classloader.PluginClassLoader>();
        var isolation = new org.pangrova.connector.runtime.classloader.DefaultConnectorClassLoaderFactory();
        assertThatThrownBy(() -> ConnectorPluginInventory.open(parent, ConnectorRole.COORDINATOR, Optional.of(root),
                (name, urls, host) -> {
                    var loader = isolation.createClassLoader(name, urls, host);
                    loaders.add(loader);
                    return loader;
                })).isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Duplicate ConnectorFactory");
        assertThat(loaders).hasSize(2).allSatisfy(loader ->
                assertThat(loader.getResource("plugin-test-resource")).isNull());
        Files.delete(other);
        Files.delete(other.getParent());
        try (var plugins = open(ConnectorRole.COORDINATOR)) {
            plugins.registry().require("test-plugin").validateConfiguration(Map.of());
        }
    }

    @Test
    void rejectsParentFactoryAccidentallyListedByPlugin() throws Exception {
        writePluginJar("descriptor-only", false);
        assertThatThrownBy(() -> open(ConnectorRole.COORDINATOR)).isInstanceOf(java.util.ServiceConfigurationError.class)
                .hasMessageContaining(TestConnectorFactory.class.getName());
    }

    @Test
    void missingConfiguredRootAndEmptyPluginFailWhileEmptyRootCanOpen() throws Exception {
        try (var plugins = open(ConnectorRole.WORKER)) {
            assertThat(plugins.registry().names()).isEmpty();
        }
        assertThatThrownBy(() -> ConnectorPluginInventory.open(parent, ConnectorRole.WORKER,
                Optional.of(root.resolve("missing")))).isInstanceOf(ConfigurationException.class);
        Files.createDirectory(root.resolve("missing-provider"));
        assertThatThrownBy(() -> open(ConnectorRole.COORDINATOR)).hasMessageContaining("No connector factory");
    }

    private ConnectorPluginInventory open(ConnectorRole role) {
        return ConnectorPluginInventory.open(parent, role, Optional.of(root));
    }

    private Path writePluginJar(String directory, boolean includeFactory) throws IOException {
        Path path = Files.createDirectory(root.resolve(directory)).resolve("connector.jar");
        try (var jar = new JarOutputStream(Files.newOutputStream(path))) {
            jar.putNextEntry(new JarEntry("META-INF/services/" + ConnectorFactory.class.getName()));
            jar.write((TestConnectorFactory.class.getName() + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
            jar.putNextEntry(new JarEntry("plugin-test-resource"));
            jar.write(1);
            jar.closeEntry();
            if (includeFactory) {
                String name = TestConnectorFactory.class.getName().replace('.', '/') + ".class";
                jar.putNextEntry(new JarEntry(name));
                try (var bytes = java.util.Objects.requireNonNull(parent.getResourceAsStream(name))) {
                    bytes.transferTo(jar);
                }
                jar.closeEntry();
            }
        }
        return path;
    }
}
