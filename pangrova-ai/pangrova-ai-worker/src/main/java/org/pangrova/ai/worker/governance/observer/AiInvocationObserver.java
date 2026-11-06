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
package org.pangrova.ai.worker.governance.observer;

import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.invocation.AiInvocationResult;

/** Receives one terminal audit event for every unified AI invocation. */
public interface AiInvocationObserver {

    void onSuccess(AiInvocationRequest request, AiInvocationResult result);

    void onFailure(AiInvocationRequest request, AiInvocationException failure);

    /** Returns an observer that deliberately discards terminal events. */
    static AiInvocationObserver noop() {
        return new AiInvocationObserver() {
            @Override
            public void onSuccess(AiInvocationRequest request, AiInvocationResult result) {}

            @Override
            public void onFailure(AiInvocationRequest request, AiInvocationException failure) {}
        };
    }
}
