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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompositeAiInvocationObserverTest {
    @Test
    void observerFanoutContinuesAfterOneObserverFails() {
        AtomicInteger delivered = new AtomicInteger();
        AiInvocationObserver failing = new AiInvocationObserver() {
            @Override
            public void onSuccess(
                    org.pangrova.spi.ai.invocation.AiInvocationRequest request,
                    org.pangrova.spi.ai.invocation.AiInvocationResult result) {
                throw new IllegalStateException("first failure");
            }

            @Override
            public void onFailure(
                    org.pangrova.spi.ai.invocation.AiInvocationRequest request,
                    org.pangrova.spi.ai.error.AiInvocationException failure) {
                throw new IllegalStateException("first failure");
            }
        };
        AiInvocationObserver succeeding = new AiInvocationObserver() {
            @Override
            public void onSuccess(
                    org.pangrova.spi.ai.invocation.AiInvocationRequest request,
                    org.pangrova.spi.ai.invocation.AiInvocationResult result) {
                delivered.incrementAndGet();
            }

            @Override
            public void onFailure(
                    org.pangrova.spi.ai.invocation.AiInvocationRequest request,
                    org.pangrova.spi.ai.error.AiInvocationException failure) {
                delivered.incrementAndGet();
            }
        };

        assertThatThrownBy(() -> CompositeAiInvocationObserver.notifyObservers(
                List.of(failing, succeeding),
                observer -> observer.onSuccess(null, null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("first failure");
        assertThat(delivered).hasValue(1);
    }
}
