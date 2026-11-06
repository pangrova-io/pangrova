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
package org.pangrova.ai.spi.model;

import org.pangrova.ai.api.model.AiModelDescriptor;
import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Map;

/**
 * Resolves the exact AI model descriptors advertised by a Worker.
 *
 * <p>The capability advertisement stays in the Worker core and must build the
 * canonical inventory without loading AI provider implementations. An installed
 * AI runtime converts its validated model configuration into descriptors; when
 * no AI runtime is present, only an unconfigured AI capability may advertise
 * no models. Configured models require exactly one catalog implementation.</p>
 */
@Experimental
public interface AiModelCatalog {

    /**
     * Returns the exact configured model descriptors.
     *
     * @param pluginConfig the AI plugin configuration properties
     * @return immutable descriptors, or an empty list when no model is configured
     */
    List<AiModelDescriptor> describeModels(Map<String, String> pluginConfig);

}
