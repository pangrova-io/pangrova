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
package org.pangrova.ai.api.invocation;

import org.pangrova.ai.api.enums.AiFinishReason;
import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.pangrova.common.util.Preconditions.requireText;

/** Immutable result returned by the unified AI invocation path. */
@Experimental
public record AiInvocationResult(
        String requestId,
        String modelName,
        String modelVersion,
        List<Output> outputs,
        String providerRequestId,
        AiFinishReason finishReason,
        Map<String, String> attributes) {

    public sealed interface Output permits
            TextOutput, VectorOutput, JsonOutput, ScoreOutput {}

    public record TextOutput(String text) implements Output {
        public TextOutput {
            Objects.requireNonNull(text, "text must not be null");
        }
    }

    public record JsonOutput(String json) implements Output {
        public JsonOutput {
            Objects.requireNonNull(json, "json must not be null");
        }
    }

    public record VectorOutput(float[] vector) implements Output {
        public VectorOutput {
            if (vector == null || vector.length == 0) {
                throw new IllegalArgumentException("vector must not be null or empty");
            }
            for (float component : vector) {
                if (!Float.isFinite(component)) {
                    throw new IllegalArgumentException(
                            "vector components must be finite");
                }
            }
            vector = vector.clone();
        }

        @Override
        public float[] vector() {
            return vector.clone();
        }
    }

    /** Higher-is-better relevance score returned by a reranking model. */
    public record ScoreOutput(double score) implements Output {
        public ScoreOutput {
            if (!Double.isFinite(score)) {
                throw new IllegalArgumentException("score must be finite");
            }
        }
    }

    public AiInvocationResult {
        requireText(requestId, "requestId");
        requireText(modelName, "modelName");
        requireText(modelVersion, "modelVersion");
        outputs = List.copyOf(Objects.requireNonNull(outputs, "outputs must not be null"));
        if (outputs.isEmpty()) {
            throw new IllegalArgumentException("outputs must not be empty");
        }
        providerRequestId = providerRequestId == null ? "" : providerRequestId;
        Objects.requireNonNull(finishReason, "finishReason must not be null");
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }
}
