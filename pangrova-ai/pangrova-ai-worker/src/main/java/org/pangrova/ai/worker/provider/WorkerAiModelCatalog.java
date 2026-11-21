/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.ai.worker.provider;

import org.pangrova.ai.api.model.AiModelCatalog;
import org.pangrova.ai.api.model.AiModelDescriptor;
import org.pangrova.ai.control.config.AiModelConfigParser;
import org.pangrova.ai.worker.config.AiFunctionActivation;

import java.util.List;
import java.util.Map;

/** Advertises the exact configured AI models without exposing provider internals. */
public final class WorkerAiModelCatalog implements AiModelCatalog {

    @Override
    public List<AiModelDescriptor> describeModels(Map<String, String> pluginConfig) {
        Map<String, String> config = pluginConfig == null ? Map.of() : pluginConfig;
        return List.copyOf(AiModelConfigParser.parse(config, AiFunctionActivation.hasFileStorage(config)).values());
    }
}
