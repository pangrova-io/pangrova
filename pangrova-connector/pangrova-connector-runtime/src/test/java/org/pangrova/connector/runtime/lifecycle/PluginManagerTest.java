package org.pangrova.connector.runtime.lifecycle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.connector.runtime.classloader.PluginClassLoader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PluginManagerTest {

    @Test
    void loadPluginsAcceptsEmptyPluginDirectory(@TempDir Path pluginDir) {
        PluginManager manager = new PluginManager(pluginDir, getClass().getClassLoader());

        manager.loadPlugins();

        assertThat(manager.getAllFactories()).isEmpty();
    }

    @Test
    void loadPluginsFailsWhenDirectoryIsMissing(@TempDir Path root) {
        PluginManager manager =
                new PluginManager(root.resolve("missing"), getClass().getClassLoader());

        assertThatThrownBy(manager::loadPlugins)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Plugin directory does not exist");
    }

    @Test
    void loadPluginsSkipsNonJarEntries(@TempDir Path pluginDir) throws Exception {
        Files.createDirectory(pluginDir.resolve("plugin-a"));
        Files.writeString(pluginDir.resolve("plugin-a").resolve("README.txt"), "not a jar");

        PluginManager manager = new PluginManager(pluginDir, getClass().getClassLoader());
        manager.loadPlugins();

        assertThat(manager.getAllFactories()).isEmpty();
        manager.unloadPlugin("plugin-a");
        manager.unloadPlugin("plugin-a");
    }

    @Test
    void discoversFactoryFromPluginJarInIsolatedClassLoader(@TempDir Path pluginDir) throws Exception {
        Path plugin = Files.createDirectory(pluginDir.resolve("test-plugin"));
        writePluginJar(plugin.resolve("test-plugin.jar"));

        PluginManager manager = new PluginManager(pluginDir, getClass().getClassLoader());
        manager.loadPlugins();

        var factory = manager.getConnectorFactory("test-plugin");
        assertThat(factory).isNotNull();
        assertThat(factory.getClass().getClassLoader()).isInstanceOf(PluginClassLoader.class);
        assertThat(factory.getClass().getClassLoader())
                .isNotSameAs(getClass().getClassLoader());

        manager.unloadPlugin("test-plugin");
        assertThat(manager.getConnectorFactory("test-plugin")).isNull();
    }

    private static void writePluginJar(Path jarPath) throws IOException {
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath))) {
            jar.putNextEntry(new JarEntry(
                    "META-INF/services/org.pangrova.connector.spi.plugin.ConnectorFactory"));
            jar.write((TestConnectorFactory.class.getName() + "\n")
                    .getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();

            String classResource = TestConnectorFactory.class.getName().replace('.', '/') + ".class";
            jar.putNextEntry(new JarEntry(classResource));
            try (InputStream bytes = TestConnectorFactory.class.getClassLoader()
                    .getResourceAsStream(classResource)) {
                if (bytes == null) {
                    throw new IllegalStateException("Missing compiled test factory: " + classResource);
                }
                bytes.transferTo(jar);
            }
            jar.closeEntry();
        }
    }
}
