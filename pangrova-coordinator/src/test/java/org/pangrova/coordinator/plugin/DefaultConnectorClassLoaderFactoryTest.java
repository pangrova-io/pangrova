package org.pangrova.coordinator.plugin;

import org.pangrova.spi.connector.classloader.SharedPackagePolicy;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link DefaultConnectorClassLoaderFactory}.
 */
class DefaultConnectorClassLoaderFactoryTest {

    private static final URL[] EMPTY_URLS = new URL[0];

    @Test
    void defaultConstructorUsesBroadPolicy() {
        DefaultConnectorClassLoaderFactory factory = new DefaultConnectorClassLoaderFactory();

        assertThat(factory.getSharedPackages())
                .containsAll(SharedPackagePolicy.BASELINE_SHARED_PACKAGES);
        assertThat(factory.getSharedPackages())
                .containsAll(SharedPackagePolicy.BROAD_EXTRA_PACKAGES);
    }

    @Test
    void narrowPolicyFactory() {
        DefaultConnectorClassLoaderFactory factory =
                new DefaultConnectorClassLoaderFactory(SharedPackagePolicy.NARROW, null);

        assertThat(factory.getSharedPackages())
                .containsExactlyInAnyOrderElementsOf(SharedPackagePolicy.BASELINE_SHARED_PACKAGES);
        assertThat(factory.getSharedPackages())
                .doesNotContainAnyElementsOf(SharedPackagePolicy.BROAD_EXTRA_PACKAGES);
    }

    @Test
    void customPolicyFactory() {
        Set<String> custom = Set.of("com.myco.lib");
        DefaultConnectorClassLoaderFactory factory =
                new DefaultConnectorClassLoaderFactory(SharedPackagePolicy.CUSTOM, custom);

        assertThat(factory.getSharedPackages()).contains("com.myco.lib");
        assertThat(factory.getSharedPackages())
                .containsAll(SharedPackagePolicy.BASELINE_SHARED_PACKAGES);
    }

    @Test
    void createClassLoaderReturnsPluginClassLoader() {
        DefaultConnectorClassLoaderFactory factory = new DefaultConnectorClassLoaderFactory();
        ClassLoader cl = factory.createClassLoader(
                "test-plugin", EMPTY_URLS, getClass().getClassLoader());

        assertThat(cl).isInstanceOf(PluginClassLoader.class);
        assertThat(((PluginClassLoader) cl).getPluginName()).isEqualTo("test-plugin");
    }

    @Test
    void eachCallCreatesDistinctClassLoader() {
        DefaultConnectorClassLoaderFactory factory = new DefaultConnectorClassLoaderFactory();
        ClassLoader cl1 = factory.createClassLoader(
                "plugin-a", EMPTY_URLS, getClass().getClassLoader());
        ClassLoader cl2 = factory.createClassLoader(
                "plugin-b", EMPTY_URLS, getClass().getClassLoader());

        assertThat(cl1).isNotSameAs(cl2);
    }

    @Test
    void nullPolicyThrowsNPE() {
        assertThatThrownBy(() -> new DefaultConnectorClassLoaderFactory(null, null))
                .isInstanceOf(NullPointerException.class);
    }
}
