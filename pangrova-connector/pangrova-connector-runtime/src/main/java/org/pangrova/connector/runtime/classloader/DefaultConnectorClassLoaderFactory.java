package org.pangrova.connector.runtime.classloader;

import org.pangrova.connector.runtime.enums.SharedPackagePolicy;

import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;
import java.util.Set;

/**
 * Default {@link ConnectorClassLoaderFactory} backed by {@link PluginClassLoader}.
 *
 * <p>The factory resolves the effective shared-package set once at construction
 * time from the configured {@link SharedPackagePolicy} and optional custom
 * packages, then reuses that set for every classloader it creates.
 *
 * <p>Role-owned external connector inventories select the narrow policy.
 */
public final class DefaultConnectorClassLoaderFactory implements ConnectorClassLoaderFactory {

    private final Set<String> sharedPackages;

    /**
     * Creates a factory with the given policy and optional custom packages.
     *
     * @param policy         the shared-package policy
     * @param customPackages custom package prefixes; only used when policy is
     *                       {@link SharedPackagePolicy#CUSTOM}
     */
    public DefaultConnectorClassLoaderFactory(SharedPackagePolicy policy, Set<String> customPackages) {
        Objects.requireNonNull(policy, "policy must not be null");
        this.sharedPackages = Set.copyOf(policy.resolveSharedPackages(customPackages));
    }

    /**
     * Convenience constructor that defaults to {@link SharedPackagePolicy#BROAD}.
     */
    public DefaultConnectorClassLoaderFactory() {
        this(SharedPackagePolicy.BROAD, null);
    }

    @Override
    public PluginClassLoader createClassLoader(String pluginName, URL[] pluginUrls,
                                         ClassLoader spiClassLoader) {
        return AccessController.doPrivileged((PrivilegedAction<PluginClassLoader>) () ->
                new PluginClassLoader(pluginName, pluginUrls, spiClassLoader, sharedPackages));
    }

    /** Returns the resolved shared-package prefixes (for diagnostics / tests). */
    public Set<String> getSharedPackages() {
        return Set.copyOf(sharedPackages);
    }
}
