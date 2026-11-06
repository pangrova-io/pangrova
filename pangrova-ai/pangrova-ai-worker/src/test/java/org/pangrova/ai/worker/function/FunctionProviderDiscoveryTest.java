package org.pangrova.ai.worker.function;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.worker.context.WorkerAiCallbackScope;
import org.pangrova.ai.spi.model.AiModelCatalog;
import org.pangrova.ai.worker.provider.WorkerAiModelCatalog;
import org.pangrova.function.spi.registry.FunctionFactory;
import org.pangrova.function.spi.runtime.FunctionCallbackScope;
import org.pangrova.function.spi.runtime.FunctionFailureProjector;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

class FunctionProviderDiscoveryTest {
    @Test
    void realProviderDescriptorsResolveThroughTheDomainContractArtifact() throws Exception {
        var factories = ServiceLoader.load(FunctionFactory.class).stream().toList();
        assertThat(factories).hasSize(1);
        try (var factory = (AiFunctionFactory) factories.get(0).get()) {
            assertThat(factory.getName()).isNotBlank();
        }
        var scopes = ServiceLoader.load(FunctionCallbackScope.class).stream().toList();
        assertThat(scopes).hasSize(1);
        assertThat(scopes.get(0).get()).isInstanceOf(WorkerAiCallbackScope.class);

        var projectors = ServiceLoader.load(FunctionFailureProjector.class).stream().toList();
        assertThat(projectors).hasSize(1);
        assertThat(projectors.get(0).get()).isInstanceOf(AiFunctionFailureProjector.class);

        var catalogs = ServiceLoader.load(AiModelCatalog.class).stream().toList();
        assertThat(catalogs).hasSize(1);
        assertThat(catalogs.get(0).get()).isInstanceOf(WorkerAiModelCatalog.class);
        assertThat(catalogs.get(0).get().describeModels(java.util.Map.of())).isEmpty();
    }
}
