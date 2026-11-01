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
package org.pangrova.spi.ai.error;

import org.pangrova.common.api.annotation.Experimental;

/** Classified, auditable failure raised by the unified AI invocation path. */
@Experimental
public final class AiInvocationException extends RuntimeException {

    public enum Category {
        INVALID_REQUEST,
        MODEL_NOT_FOUND,
        MODEL_DISABLED,
        CAPABILITY_MISMATCH,
        AUTHENTICATION,
        AUTHORIZATION,
        RATE_LIMIT,
        TIMEOUT,
        PROVIDER_UNAVAILABLE,
        PROVIDER_OUTCOME_UNCERTAIN,
        PROVIDER_RESPONSE_INVALID,
        CONTENT_REJECTED,
        BUDGET_EXCEEDED,
        INTERNAL
    }

    private final Category category;
    private final boolean retryable;

    public AiInvocationException(Category category, boolean retryable, String message) {
        super(message);
        this.category = java.util.Objects.requireNonNull(category, "category must not be null");
        this.retryable = retryable;
    }

    public AiInvocationException(
            Category category, boolean retryable, String message, Throwable cause) {
        super(message, cause);
        this.category = java.util.Objects.requireNonNull(category, "category must not be null");
        this.retryable = retryable;
    }

    public Category category() {
        return category;
    }

    public boolean retryable() {
        return retryable;
    }
}
