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
package org.pangrova.worker.ai.invocation;

import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.spi.ai.AiInvocationRequest;
import org.pangrova.worker.ai.context.AiExecutionContext;
import org.pangrova.worker.ai.context.BoundAiInvocation;

/** Validates trusted task identity and exact operator grants before admission. */
final class AiInvocationBudgetValidator {

    private AiInvocationBudgetValidator() {}

    static void validate(
            AiExecutionContext context,
            AiInvocationRequest request,
            BoundAiInvocation binding) {
        validateTrustedContext(context, request, binding);
        validateOperatorGrant(context, binding);
    }

    private static void validateOperatorGrant(
            AiExecutionContext context, BoundAiInvocation binding) {
        if (binding.maxProviderConcurrency() > context.maxProviderConcurrency()
                || binding.maxProviderItems() > context.maxProviderItems()
                || binding.maxProviderAttempts() > context.maxProviderAttempts()
                || binding.maxProviderRequestBytes() > context.maxProviderRequestBytes()
                || binding.maxProviderResponseBytes() > context.maxProviderResponseBytes()
                || binding.maxMediaBytes() > context.maxMediaBytes()) {
            throw authorization(
                    "AI operator resource envelope exceeds its native task grant",
                    null);
        }
    }

    private static void validateTrustedContext(
            AiExecutionContext context,
            AiInvocationRequest request,
            BoundAiInvocation binding) {
        try {
            context.requireGovernedIdentity();
            context.requireProviderBudget();
        } catch (IllegalStateException failure) {
            throw authorization(failure.getMessage(), failure);
        }
        if (!hasSameSecurityIdentity(context, request)
                || !hasSameExecutionIdentity(context, request)
                || !hasSameDeadline(context, request)
                || !hasBoundIdempotency(request, binding)) {
            throw authorization(
                    "AI invocation identity differs from its trusted native task context",
                    null);
        }
    }

    private static boolean hasSameSecurityIdentity(
            AiExecutionContext context, AiInvocationRequest request) {
        return context.tenantId().equals(request.tenantId())
                && context.principal().equals(request.principal());
    }

    private static boolean hasSameExecutionIdentity(
            AiExecutionContext context, AiInvocationRequest request) {
        return context.queryId().equals(request.queryId())
                && context.traceId().equals(request.traceId())
                && context.taskId().equals(request.attributes().get("task_id"))
                && java.util.Base64.getEncoder().encodeToString(context.taskAttempt().toByteArray())
                        .equals(request.attributes().get("task_attempt_base64"));
    }

    private static boolean hasSameDeadline(
            AiExecutionContext context, AiInvocationRequest request) {
        return context.deadline().isPresent()
                && context.deadlineEpochMs() == request.deadline().toEpochMilli();
    }

    private static boolean hasBoundIdempotency(
            AiInvocationRequest request, BoundAiInvocation binding) {
        String prefix = binding.idempotencyBase() + ":";
        String value = request.idempotencyKey();
        return value.startsWith(prefix)
                && value.substring(prefix.length()).matches("[0-9a-f]{64}");
    }

    private static AiInvocationException authorization(
            String message, Throwable cause) {
        return new AiInvocationException(
                AiInvocationException.Category.AUTHORIZATION,
                false,
                message,
                cause);
    }
}
