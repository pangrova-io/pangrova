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

import org.pangrova.ai.worker.governance.observer.AiInvocationObserver;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.invocation.AiInvocationResult;

import java.util.Objects;

/** Emits exactly one terminal audit outcome for a governed invocation. */
final class AiInvocationTerminalReporter {

    private final AiInvocationObserver observer;

    AiInvocationTerminalReporter(AiInvocationObserver observer) {
        this.observer = Objects.requireNonNull(observer, "observer required");
    }

    void success(AiInvocationRequest request, AiInvocationResult result) {
        try {
            observer.onSuccess(request, result);
        } catch (RuntimeException failure) {
            throw terminalFailure(request, "terminal audit", failure);
        }
    }

    void failure(
            AiInvocationRequest request, AiInvocationException failure) {
        try {
            observer.onFailure(request, failure);
        } catch (RuntimeException observerFailure) {
            if (observerFailure != failure) {
                failure.addSuppressed(observerFailure);
            }
        }
    }

    private static AiInvocationException terminalFailure(
            AiInvocationRequest request, String stage, RuntimeException failure) {
        return new AiInvocationException(
                AiInvocationException.Category.INTERNAL,
                false,
                "AI invocation " + request.requestId()
                        + " failed during " + stage
                        + " after provider completion",
                failure);
    }
}
