package org.pangrova.connector.runtime.classloader;

import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PluginClassLoader} delegation behaviour with SPI-first,
 * shared-first, and plugin-first strategies.
 */
class PluginClassLoaderTest {

    private static final URL[] EMPTY_URLS = new URL[0];

    @Test
    void spiClassesDelegateToSpiClassLoader() throws ClassNotFoundException {
        // org.pangrova.spi.connector.plugin.Connector is an SPI class — must always
        // be loaded from the SPI classloader regardless of policy.
        PluginClassLoader cl = new PluginClassLoader(
                "test", EMPTY_URLS, getClass().getClassLoader());

        Class<?> loaded = cl.loadClass("org.pangrova.spi.connector.plugin.Connector");
        assertThat(loaded).isSameAs(org.pangrova.spi.connector.plugin.Connector.class);
    }

    @Test
    void sharedClassesDelegateToSpiClassLoaderFirst() throws ClassNotFoundException {
        // SLF4J is in the shared packages — should be resolvable from the
        // SPI classloader (which is our test classloader here).
        PluginClassLoader cl = new PluginClassLoader(
                "test-shared", EMPTY_URLS, getClass().getClassLoader());

        // org.slf4j.Logger is on the test classpath and in SHARED_PACKAGES
        Class<?> loaded = cl.loadClass("org.slf4j.Logger");
        assertThat(loaded).isNotNull();
    }

    @Test
    void pluginNameIsAccessible() {
        PluginClassLoader cl = new PluginClassLoader(
                "my-plugin", EMPTY_URLS, getClass().getClassLoader());

        assertThat(cl.getPluginName()).isEqualTo("my-plugin");
    }

    @Test
    void jdkClassesAreAlwaysAccessible() throws ClassNotFoundException {
        PluginClassLoader cl = new PluginClassLoader(
                "test", EMPTY_URLS, getClass().getClassLoader());

        // java.lang.String is always loadable (bootstrap classloader)
        Class<?> loaded = cl.loadClass("java.lang.String");
        assertThat(loaded).isSameAs(String.class);
    }

    @Test
    void commonModuleClassesDelegateToSpiClassLoader() throws ClassNotFoundException {
        // org.pangrova.common classes are SPI packages — must delegate
        PluginClassLoader cl = new PluginClassLoader(
                "test-common", EMPTY_URLS, getClass().getClassLoader());

        // Load any class from org.pangrova.common package
        Class<?> loaded = cl.loadClass("org.pangrova.common.util.Preconditions");
        assertThat(loaded).isNotNull();
    }
}
