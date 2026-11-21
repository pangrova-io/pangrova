package org.pangrova.ai.worker.provider;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.invocation.RerankingBatchResult;
import org.pangrova.ai.api.reranking.RerankingProvider;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VersionedAiProvidersTest {
    @Test
    void versionBindingReleasesAutoCloseablePluginAndPreservesItsCheckedFailure() {
        Exception failure = new Exception("remote session release failed");
        class ModelSession implements RerankingProvider, AutoCloseable {
            private int attempts;
            @Override public RerankingBatchResult score(String query, List<String> documents) {
                return new RerankingBatchResult(List.of(1.0), "request");
            }
            @Override public String modelName() { return "ranking"; }
            @Override public int maxBatchSize() { return 1; }
            @Override public void close() throws Exception {
                attempts++;
                throw failure;
            }
        }
        var plugin = new ModelSession();
        var binding = VersionedAiProviders.reranking(plugin, "ranking", "v1", new byte[32], Set.of());
        assertThatThrownBy(binding::close).isInstanceOf(IOException.class).hasCause(failure);
        assertThat(plugin.attempts).isOne();
    }
}
