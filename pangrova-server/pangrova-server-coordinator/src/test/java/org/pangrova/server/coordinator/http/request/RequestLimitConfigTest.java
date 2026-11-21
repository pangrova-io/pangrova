package org.pangrova.server.coordinator.http.request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RequestLimitConfig}.
 */
class RequestLimitConfigTest {

    @Test
    void defaultsReturnsExpectedValues() {
        RequestLimitConfig config = RequestLimitConfig.defaults();

        assertThat(config.maxRequestSizeBytes()).isEqualTo(1_048_576);
        assertThat(config.maxRequestsPerMinute()).isEqualTo(100);
    }

    @Test
    void customValuesArePreserved() {
        RequestLimitConfig config = new RequestLimitConfig(2_000_000, 200);

        assertThat(config.maxRequestSizeBytes()).isEqualTo(2_000_000);
        assertThat(config.maxRequestsPerMinute()).isEqualTo(200);
    }

    @Test
    void rejectsZeroMaxRequestSize() {
        assertThatThrownBy(() -> new RequestLimitConfig(0, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRequestSizeBytes must be positive");
    }

    @Test
    void rejectsNegativeMaxRequestSize() {
        assertThatThrownBy(() -> new RequestLimitConfig(-1, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRequestSizeBytes must be positive");
    }

    @Test
    void rejectsZeroMaxRequestsPerMinute() {
        assertThatThrownBy(() -> new RequestLimitConfig(1024, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRequestsPerMinute must be positive");
    }

    @Test
    void rejectsNegativeMaxRequestsPerMinute() {
        assertThatThrownBy(() -> new RequestLimitConfig(1024, -5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRequestsPerMinute must be positive");
    }
}
