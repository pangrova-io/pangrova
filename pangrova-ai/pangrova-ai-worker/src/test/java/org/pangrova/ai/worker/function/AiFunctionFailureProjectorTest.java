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
package org.pangrova.ai.worker.function;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.error.AiInvocationException;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;

import static org.assertj.core.api.Assertions.assertThat;

class AiFunctionFailureProjectorTest {

    private final AiFunctionFailureProjector projector = new AiFunctionFailureProjector();

    @Test
    void uncertainProviderOutcomeProjectsToNonRetryableNativeTaskFailure() throws Exception {
        var failure = new AiInvocationException(
                AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                true,
                "response lost after provider accepted POST");

        var projected = ExecutionError.parseFrom(projector.describeFailure(failure));

        assertThat(projected.getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_PROVIDER_OUTCOME_UNCERTAIN);
        assertThat(projected.getCode()).isEqualTo("AI_PROVIDER_OUTCOME_UNCERTAIN");
        assertThat(projected.getMessage()).isEqualTo(failure.getMessage());
        assertThat(projected.getRetryable()).isFalse();
        assertThat(projected.getFailedComponent()).isEqualTo("ai-invocation");
        assertThat(projector.describeFailure(new IllegalStateException("ordinary UDF error"))).isNull();
    }

    @Test
    void wrappedInvocationFailureStillProjectsToTheCanonicalContract() throws Exception {
        var failure = new AiInvocationException(
                AiInvocationException.Category.TIMEOUT, true, "provider timed out");
        var wrapped = new IllegalStateException("Batch evaluation failed", failure);

        var projected = ExecutionError.parseFrom(projector.describeFailure(wrapped));

        assertThat(projected.getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_DEADLINE);
        assertThat(projected.getCode()).isEqualTo("AI_TIMEOUT");
        assertThat(projected.getMessage()).isEqualTo("provider timed out");
        assertThat(projected.getRetryable()).isTrue();
    }

    @Test
    void largeProviderMessageKeepsTheCanonicalNativeFailureBounded() throws Exception {
        var failure = new AiInvocationException(
                AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                false,
                "x".repeat(100_000));

        byte[] encoded = projector.describeFailure(failure);
        var projected = ExecutionError.parseFrom(encoded);

        assertThat(encoded.length).isLessThan(65_536);
        assertThat(projected.getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_PROVIDER_OUTCOME_UNCERTAIN);
        assertThat(projected.getMessage()).endsWith("[truncated]");
        assertThat(projected.getRetryable()).isFalse();
    }
}
