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
package org.pangrova.shuffle.celeborn;

import java.util.Map;

import org.pangrova.shuffle.ShuffleContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CelebornShuffleProviderBuilderTest {

    private static final String MASTER_ENDPOINTS = "celeborn.master.endpoints";

    @AfterEach
    void clearSystemProperty() {
        System.clearProperty(MASTER_ENDPOINTS);
    }

    @Test
    void missingMasterEndpointFailsFast() {
        ShuffleContext context = new ShuffleContext("celeborn", Map.of());

        assertThatThrownBy(() -> new CelebornShuffleProviderBuilder().apply(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(MASTER_ENDPOINTS + " must be configured, e.g. \"host1:9097\" "
                        + "or \"host1:9097,host2:9097\"");
    }

    /**
     * The message must carry a concrete example: the default was removed, so this
     * exception is now the only place an operator learns the expected format.
     */
    @Test
    void failureMessageCarriesAnExampleEndpoint() {
        ShuffleContext context = new ShuffleContext("celeborn", Map.of());

        assertThatThrownBy(() -> new CelebornShuffleProviderBuilder().apply(context))
                .hasMessageContaining("host1:9097");
    }

    @Test
    void blankMasterEndpointFailsFast() {
        ShuffleContext context = new ShuffleContext("celeborn", Map.of(MASTER_ENDPOINTS, "   "));

        assertThatThrownBy(() -> new CelebornShuffleProviderBuilder().apply(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(MASTER_ENDPOINTS + " must be configured");
    }
}
