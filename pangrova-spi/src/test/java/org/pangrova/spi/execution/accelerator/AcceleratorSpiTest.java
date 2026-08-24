package org.pangrova.spi.execution.accelerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the hardware accelerator offload SPI contracts.
 */
class AcceleratorSpiTest {

    // ---- AcceleratorType ----

    @Test
    void acceleratorTypeContainsExpectedValues() {
        AcceleratorType[] values = AcceleratorType.values();
        assertThat(values).containsExactly(
                AcceleratorType.CPU,
                AcceleratorType.GPU_CUDA,
                AcceleratorType.GPU_OPENCL,
                AcceleratorType.FPGA,
                AcceleratorType.TPU);
    }

    @Test
    void acceleratorTypeValueOfRoundTrips() {
        for (AcceleratorType type : AcceleratorType.values()) {
            assertThat(AcceleratorType.valueOf(type.name())).isEqualTo(type);
        }
    }

    // ---- AcceleratorCapabilities ----

    @Test
    void capabilitiesRecordStoresFields() {
        Set<String> ops = Set.of("hash_join", "aggregation");
        AcceleratorCapabilities caps = new AcceleratorCapabilities(ops, 1024, 4096);

        assertThat(caps.supportedOperators()).containsExactlyInAnyOrder("hash_join", "aggregation");
        assertThat(caps.maxBatchSizeBytes()).isEqualTo(1024);
        assertThat(caps.memoryLimitBytes()).isEqualTo(4096);
    }

    @Test
    void capabilitiesSupportsOperatorQuery() {
        AcceleratorCapabilities caps = new AcceleratorCapabilities(
                Set.of("hash_join"), 1024, 4096);

        assertThat(caps.supportsOperator("hash_join")).isTrue();
        assertThat(caps.supportsOperator("sort")).isFalse();
    }

    @Test
    void capabilitiesNullOperatorsDefaultsToEmpty() {
        AcceleratorCapabilities caps = new AcceleratorCapabilities(null, 0, 0);
        assertThat(caps.supportedOperators()).isEmpty();
    }

    @Test
    void capabilitiesRejectsNegativeBatchSize() {
        assertThatThrownBy(() -> new AcceleratorCapabilities(Set.of(), -1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchSizeBytes");
    }

    @Test
    void capabilitiesRejectsNegativeMemoryLimit() {
        assertThatThrownBy(() -> new AcceleratorCapabilities(Set.of(), 0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("memoryLimitBytes");
    }

    @Test
    void capabilitiesOperatorsAreDefensivelyCopied() {
        var mutable = new java.util.HashSet<>(Set.of("sort"));
        AcceleratorCapabilities caps = new AcceleratorCapabilities(mutable, 1024, 4096);
        mutable.add("filter");

        assertThat(caps.supportedOperators()).containsExactly("sort");
    }

    // ---- AcceleratorProvider & AcceleratorSession (stub impl) ----

    @Test
    void stubProviderReportsTypeAndAvailability() {
        AcceleratorProvider provider = new StubAcceleratorProvider(true);

        assertThat(provider.type()).isEqualTo(AcceleratorType.GPU_CUDA);
        assertThat(provider.isAvailable()).isTrue();
    }

    @Test
    void stubProviderUnavailable() {
        AcceleratorProvider provider = new StubAcceleratorProvider(false);

        assertThat(provider.isAvailable()).isFalse();
    }

    @Test
    void sessionExecuteReturnsResult() {
        AcceleratorProvider provider = new StubAcceleratorProvider(true);
        try (AcceleratorSession session = provider.createSession()) {
            ByteBuffer input = ByteBuffer.wrap(new byte[]{0x01, 0x02});
            ByteBuffer result = session.execute("hash_join", input);

            assertThat(result).isNotNull();
            assertThat(result.remaining()).isEqualTo(2);
        }
    }

    @Test
    void sessionCloseReleasesResources() {
        StubAcceleratorProvider provider = new StubAcceleratorProvider(true);
        StubAcceleratorSession session = (StubAcceleratorSession) provider.createSession();

        assertThat(session.isClosed()).isFalse();
        session.close();
        assertThat(session.isClosed()).isTrue();
    }

    @Test
    void sessionExecuteAfterCloseThrows() {
        AcceleratorProvider provider = new StubAcceleratorProvider(true);
        AcceleratorSession session = provider.createSession();
        session.close();

        assertThatThrownBy(() -> session.execute("hash_join", ByteBuffer.allocate(1)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void sessionExecuteUnsupportedOperatorThrows() {
        AcceleratorProvider provider = new StubAcceleratorProvider(true);
        try (AcceleratorSession session = provider.createSession()) {
            assertThatThrownBy(() -> session.execute("unknown_op", ByteBuffer.allocate(1)))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void providerCapabilitiesMatchSession() {
        AcceleratorProvider provider = new StubAcceleratorProvider(true);
        AcceleratorCapabilities caps = provider.capabilities();

        assertThat(caps.supportedOperators()).contains("hash_join");
        assertThat(caps.maxBatchSizeBytes()).isGreaterThan(0);
        assertThat(caps.memoryLimitBytes()).isGreaterThan(0);
    }

    // ---- Stub implementations for testing SPI contracts ----

    private static final class StubAcceleratorProvider implements AcceleratorProvider {

        private static final AcceleratorCapabilities CAPS = new AcceleratorCapabilities(
                Set.of("hash_join", "aggregation"), 64 * 1024, 256 * 1024 * 1024L);

        private final boolean available;

        StubAcceleratorProvider(boolean available) {
            this.available = available;
        }

        @Override
        public AcceleratorType type() {
            return AcceleratorType.GPU_CUDA;
        }

        @Override
        public boolean isAvailable() {
            return available;
        }

        @Override
        public AcceleratorCapabilities capabilities() {
            if (!available) {
                throw new IllegalStateException("Accelerator not available");
            }
            return CAPS;
        }

        @Override
        public AcceleratorSession createSession() {
            if (!available) {
                throw new IllegalStateException("Accelerator not available");
            }
            return new StubAcceleratorSession(CAPS);
        }
    }

    private static final class StubAcceleratorSession implements AcceleratorSession {

        private final AcceleratorCapabilities capabilities;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        StubAcceleratorSession(AcceleratorCapabilities capabilities) {
            this.capabilities = capabilities;
        }

        @Override
        public ByteBuffer execute(String operatorName, ByteBuffer input) {
            if (closed.get()) {
                throw new IllegalStateException("Session is closed");
            }
            if (!capabilities.supportsOperator(operatorName)) {
                throw new UnsupportedOperationException(
                        "Unsupported operator: " + operatorName);
            }
            // Stub: echo back the input unchanged
            byte[] data = new byte[input.remaining()];
            input.get(data);
            return ByteBuffer.wrap(data);
        }

        @Override
        public void close() {
            closed.set(true);
        }

        boolean isClosed() {
            return closed.get();
        }
    }
}
