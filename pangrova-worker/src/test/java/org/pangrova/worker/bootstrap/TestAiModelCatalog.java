package org.pangrova.worker.bootstrap;

import org.pangrova.ai.control.config.AiModelConfigParser;
import org.pangrova.ai.api.model.AiModelDescriptor;
import org.pangrova.ai.api.model.AiModelCatalog;

import java.util.List;
import java.util.Map;

/**
 * Test harness standing in for an installed AI runtime on the Worker unit-test
 * classpath. It converts configuration with the same canonical parser the
 * production catalog uses, so capability advertisement is exercised against the
 * real descriptor contract without depending on AI provider implementations.
 */
public final class TestAiModelCatalog implements AiModelCatalog {

    @Override
    public List<AiModelDescriptor> describeModels(Map<String, String> pluginConfig) {
        Map<String, String> config = pluginConfig == null ? Map.of() : pluginConfig;
        String backend = config.get("file.storage-backend");
        return List.copyOf(AiModelConfigParser.parse(
                config, backend != null && !backend.isBlank()).values());
    }
}
