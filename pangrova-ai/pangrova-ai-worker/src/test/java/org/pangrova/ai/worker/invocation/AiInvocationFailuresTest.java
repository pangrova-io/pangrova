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
package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.api.enums.AiFailureCategory;

import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.ai.api.exception.AiInvocationException;

import static org.assertj.core.api.Assertions.assertThat;

class AiInvocationFailuresTest {

    @Test
    void mapsGovernedAiAuthorizationDenialWithoutLosingItsCategory() {
        AiInvocationException mapped = AiInvocationFailures.classify(
                new PangrovaException(
                        PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED,
                        "blob principal does not match invocation principal"));

        assertThat(mapped.category()).isEqualTo(AiFailureCategory.AUTHORIZATION);
        assertThat(mapped.retryable()).isFalse();
        assertThat(mapped.getCause()).isInstanceOf(PangrovaException.class);
    }

    @Test
    void mapsEmbeddingContractViolationsAsInvalidProviderResponses() {
        AiInvocationException mapped = AiInvocationFailures.classify(
                new PangrovaException(
                        PangrovaErrorCode.AI_EMBEDDING_DIMENSION_MISMATCH,
                        "non-finite embedding"));

        assertThat(mapped.category())
                .isEqualTo(AiFailureCategory.PROVIDER_RESPONSE_INVALID);
        assertThat(mapped.retryable()).isFalse();
    }
}
