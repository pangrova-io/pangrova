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

import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.function.api.runtime.FunctionFailureProjector;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;

/**
 * Projects AI invocation failures raised inside a Native function callback
 * into the canonical Native task error contract.
 *
 * <p>The Worker function bridge re-raises the callback failure unchanged and
 * may wrap it in a generic function failure. This projector therefore walks the
 * cause chain to find the classified {@link AiInvocationException} while
 * leaving non-AI failures unprojected.</p>
 */
public final class AiFunctionFailureProjector implements FunctionFailureProjector {

    private static final int MAX_FAILURE_MESSAGE_CHARS = 8_192;
    private static final int MAX_CAUSE_DEPTH = 64;

    @Override
    public byte[] describeFailure(Throwable failure) {
        AiInvocationException aiFailure = findAiFailure(failure);
        if (aiFailure == null) {
            return null;
        }
        ErrorClass errorClass = switch (aiFailure.category()) {
            case INVALID_REQUEST, MODEL_NOT_FOUND -> ErrorClass.ERROR_CLASS_INVALID_REQUEST;
            case CAPABILITY_MISMATCH -> ErrorClass.ERROR_CLASS_CAPABILITY_MISMATCH;
            case AUTHENTICATION, AUTHORIZATION, MODEL_DISABLED, CONTENT_REJECTED ->
                    ErrorClass.ERROR_CLASS_POLICY_DENIED;
            case RATE_LIMIT -> ErrorClass.ERROR_CLASS_ADMISSION;
            case TIMEOUT -> ErrorClass.ERROR_CLASS_DEADLINE;
            case BUDGET_EXCEEDED -> ErrorClass.ERROR_CLASS_BUDGET_EXHAUSTED;
            case PROVIDER_OUTCOME_UNCERTAIN -> ErrorClass.ERROR_CLASS_PROVIDER_OUTCOME_UNCERTAIN;
            case PROVIDER_UNAVAILABLE, PROVIDER_RESPONSE_INVALID -> ErrorClass.ERROR_CLASS_PROVIDER;
            case INTERNAL -> ErrorClass.ERROR_CLASS_INTERNAL;
        };
        String message = aiFailure.getMessage();
        if (message != null && message.length() > MAX_FAILURE_MESSAGE_CHARS) {
            message = message.substring(0, MAX_FAILURE_MESSAGE_CHARS) + " [truncated]";
        }
        return ExecutionError.newBuilder()
                .setErrorClass(errorClass)
                .setCode("AI_" + aiFailure.category().name())
                .setMessage(message == null || message.isBlank()
                        ? aiFailure.category().name()
                        : message)
                .setRetryable(aiFailure.category() != AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN
                        && aiFailure.retryable())
                .setFailedComponent("ai-invocation")
                .build()
                .toByteArray();
    }

    private static AiInvocationException findAiFailure(Throwable failure) {
        Throwable current = failure;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (current instanceof AiInvocationException aiFailure) {
                return aiFailure;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                return null;
            }
            current = cause;
        }
        return null;
    }
}
