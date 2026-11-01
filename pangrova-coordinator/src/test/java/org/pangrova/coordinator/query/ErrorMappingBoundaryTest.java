package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.admission.QueryRejectedException;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.exception.ValidationException;
import org.pangrova.coordinator.scheduler.failure.CircuitBreakerOpenException;
import org.pangrova.spi.transport.ResultTooLargeException;
import org.pangrova.worker.exchange.ExchangeConnectionException;
import org.pangrova.shuffle.api.exchange.ExchangeEndpoint;
import org.pangrova.worker.jni.NativeRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies that all module-boundary exceptions extend PangrovaException
 * and carry the correct PangrovaErrorCode. No internal exception types
 * leak across module boundaries.
 *
 * Validates: Requirements 5.8
 */
@DisplayName("Error Mapping at Module Boundaries")
class ErrorMappingBoundaryTest {

    @Test
    @DisplayName("CircuitBreakerOpenException carries COORDINATOR_CIRCUIT_BREAKER_OPEN")
    void circuitBreakerOpenException() {
        var ex = new CircuitBreakerOpenException("worker-1");
        assertPangrovaException(ex, PangrovaErrorCode.COORDINATOR_CIRCUIT_BREAKER_OPEN);
        assertThat(ex.getWorkerHost()).isEqualTo("worker-1");
    }

    @Test
    @DisplayName("ExchangeConnectionException carries EXCHANGE_CONNECTION_FAILED")
    void exchangeConnectionException() {
        var source = new ExchangeEndpoint(
                "task-1", 0, 0, 0, 1L, 0, (1L << 32) + 1L,
                "host-1", 9090, "", new byte[] {1},
                System.currentTimeMillis() + 60_000L, 0L, 0L);
        var cause = new java.io.IOException("connection refused");
        var ex = new ExchangeConnectionException(source, 3, cause);
        assertPangrovaException(ex, PangrovaErrorCode.EXCHANGE_CONNECTION_FAILED);
        assertThat(ex.getSourceTaskId()).isEqualTo("task-1");
        assertThat(ex.getAttempts()).isEqualTo(3);
    }

    @Test
    @DisplayName("ResultTooLargeException carries TRANSPORT_RESULT_TOO_LARGE")
    void resultTooLargeException() {
        var ex = new ResultTooLargeException(1024, 2048);
        assertPangrovaException(ex, PangrovaErrorCode.TRANSPORT_RESULT_TOO_LARGE);
        assertThat(ex.getMaxBytes()).isEqualTo(1024);
        assertThat(ex.getActualBytes()).isEqualTo(2048);
    }

    @Test
    @DisplayName("NativeRuntimeException carries NATIVE_JNI_EXCEPTION by default")
    void nativeRuntimeException() {
        var ex = new NativeRuntimeException("JNI call failed");
        assertPangrovaException(ex, PangrovaErrorCode.NATIVE_JNI_EXCEPTION);
    }

    @Test
    @DisplayName("ValidationException carries correct error code")
    void validationException() {
        var ex = new ValidationException(
                PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT, "bad column");
        assertPangrovaException(ex, PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT);
    }

    @Test
    @DisplayName("All boundary exceptions are instanceof PangrovaException")
    void allBoundaryExceptionsExtendPangrovaException() {
        assertThat(PangrovaException.class)
                .isAssignableFrom(CircuitBreakerOpenException.class);
        assertThat(PangrovaException.class)
                .isAssignableFrom(ExchangeConnectionException.class);
        assertThat(PangrovaException.class)
                .isAssignableFrom(ResultTooLargeException.class);
        assertThat(PangrovaException.class)
                .isAssignableFrom(NativeRuntimeException.class);
        assertThat(PangrovaException.class)
                .isAssignableFrom(ValidationException.class);
        assertThat(PangrovaException.class)
                .isAssignableFrom(QueryRejectedException.class);
    }

    private static void assertPangrovaException(PangrovaException ex, PangrovaErrorCode expectedCode) {
        assertThat(ex).isInstanceOf(PangrovaException.class);
        assertThat(ex.getErrorCode()).isEqualTo(expectedCode);
        assertThat(ex.getMessage()).isNotBlank();
    }
}
