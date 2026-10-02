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
package org.pangrova.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Validates P2-7: the {@link FlightResultFetcher} Arrow allocator upper bound is a
 * configurable hard cap (defaulting to 512 MiB), so client-side result buffering
 * cannot grow unbounded and can be tuned per deployment.
 */
class FlightResultFetcherAllocatorTest {

    private static final long MIB = 1024L * 1024L;

    @Test
    void defaultAllocatorLimitIs512MiB() {
        try (FlightResultFetcher fetcher = new FlightResultFetcher()) {
            assertThat(fetcher.allocatorLimitBytes()).isEqualTo(512 * MIB);
        }
    }

    @Test
    void configuredAllocatorLimitIsHonored() {
        try (FlightResultFetcher fetcher = new FlightResultFetcher(false, null, 64 * MIB)) {
            assertThat(fetcher.allocatorLimitBytes()).isEqualTo(64 * MIB);
        }
    }

    @Test
    void nonPositiveAllocatorLimitRejected() {
        assertThatThrownBy(() -> new FlightResultFetcher(false, null, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allocatorLimitBytes");
    }

    @Test
    void expiredCallerDeadlineDoesNotWaitForFlightIdleTimeout() {
        assertThat(FlightResultFetcher.readWaitNanos(
                System.currentTimeMillis() - 1)).isZero();
    }

    @Test
    void callerDeadlineBoundsEachFlightBatchRead() {
        long remainingNanos = FlightResultFetcher.readWaitNanos(
                System.currentTimeMillis() + 1_000);
        assertThat(remainingNanos).isPositive()
                .isLessThanOrEqualTo(java.util.concurrent.TimeUnit.SECONDS.toNanos(1));
        assertThat(FlightResultFetcher.readWaitNanos(Long.MAX_VALUE))
                .isEqualTo(java.util.concurrent.TimeUnit.SECONDS.toNanos(120));
    }
}
