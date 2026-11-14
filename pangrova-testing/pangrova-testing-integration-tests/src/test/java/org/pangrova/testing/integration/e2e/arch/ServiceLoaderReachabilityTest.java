package org.pangrova.testing.integration.e2e.arch;

import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.runtime.discovery.ConnectorFactoryRegistry;
import org.pangrova.security.api.secret.SecretProvider;
import org.pangrova.metadata.api.catalog.FederatedCatalogFactory;
import org.pangrova.function.api.registry.FunctionFactory;
import org.pangrova.function.api.runtime.FunctionCallbackScope;
import org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;
import org.pangrova.metadata.catalog.federation.PluginFederationHandleFactory;

import java.util.ArrayDeque;
import java.util.Set;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ServiceLoaderReachabilityTest {
    @Test
    void realBootstrapFindsFunctionsCallbacksAndFailureProjectors() {
        var classes = new ClassFileImporter().importClasses(NativeFunctionBootstrap.class);
        var providers = new ArrayDeque<String>();
        ServiceLoaderReachability.enqueueProviders(NativeFunctionBootstrap.class.getName(), classes, providers, Set.of());
        assertThat(providers).contains("org.pangrova.ai.worker.function.AiFunctionFactory",
                "org.pangrova.ai.worker.context.WorkerAiCallbackScope",
                "org.pangrova.ai.worker.function.AiFunctionFailureProjector");
    }

    @Test
    void realConnectorDiscoveryFollowsTheRegistryLoadSite() {
        var classes = new ClassFileImporter().importClasses(ConnectorFactoryRegistry.class);
        var providers = new ArrayDeque<String>();
        ServiceLoaderReachability.enqueueProviders(ConnectorFactoryRegistry.class.getName(), classes, providers, Set.of());
        assertThat(providers).contains("org.pangrova.connector.iceberg.IcebergConnectorFactory",
                "org.pangrova.connector.lance.LanceConnectorFactory",
                "org.pangrova.connector.paimon.PaimonConnectorFactory");
    }

    @Test
    void constructorDiscoveryUsesItsActualServiceArgument() {
        var classes = new ClassFileImporter().importClasses(
                SecretResolverRegistry.class, PluginFederationHandleFactory.class);
        assertThat(ServiceLoaderReachability.contractsOf(SecretResolverRegistry.class.getName(), classes))
                .containsExactly(SecretProvider.class.getName());
        assertThat(ServiceLoaderReachability.contractsOf(PluginFederationHandleFactory.class.getName(), classes))
                .containsExactly(FederatedCatalogFactory.class.getName());
    }

    @Test
    void unrelatedClassLiteralInTheSameMethodDoesNotDiscoverItsProviders() {
        var classes = new ClassFileImporter().importClasses(OnlyCallbacks.class);
        var providers = new ArrayDeque<String>();
        ServiceLoaderReachability.enqueueProviders(OnlyCallbacks.class.getName(), classes, providers, Set.of());
        assertThat(providers).containsExactly("org.pangrova.ai.worker.context.WorkerAiCallbackScope");
    }

    @Test
    void multilineLiteralStillIdentifiesTheActualContract() {
        var classes = new ClassFileImporter().importClasses(Multiline.class);
        var providers = new ArrayDeque<String>();
        ServiceLoaderReachability.enqueueProviders(Multiline.class.getName(), classes, providers, Set.of());
        assertThat(providers).containsExactly("org.pangrova.ai.worker.context.WorkerAiCallbackScope");
    }

    @Test
    void missingOrDynamicDiscoveryCannotCreateProviderRoots() {
        var classes = new ClassFileImporter().importClasses(NoLoader.class, DynamicLoader.class);
        var providers = new ArrayDeque<String>();
        ServiceLoaderReachability.enqueueProviders(NoLoader.class.getName(), classes, providers, Set.of());
        assertThat(providers).isEmpty();
        assertThatThrownBy(() -> ServiceLoaderReachability.enqueueProviders(
                DynamicLoader.class.getName(), classes, providers, Set.of()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("Cannot identify ServiceLoader contract");
        assertThat(providers).isEmpty();
    }

    static class OnlyCallbacks {
        static Class<?> discover() {
            Class<?> unrelated = FunctionFactory.class;
            ServiceLoader.load(FunctionCallbackScope.class);
            return unrelated;
        }
    }

    static class Multiline {
        static void discover() {
            ServiceLoader.load(
                    FunctionCallbackScope.class);
        }
    }

    static class NoLoader {
        static Class<?> contract() {
            return FunctionCallbackScope.class;
        }
    }

    static class DynamicLoader {
        static <T> Class<?> discover(Class<T> service) {
            Class<?> unrelated = FunctionCallbackScope.class;
            ServiceLoader.load(service);
            return unrelated;
        }
    }
}
