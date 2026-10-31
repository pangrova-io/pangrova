package org.pangrova.observability.sdk.logging.provider.mdc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * Verifies {@link LogContext} MDC wiring for trace correlation.
 */
class LogContextTest {

    @AfterEach
    void cleanup() {
        LogContext.clear();
    }

    @Test
    void set_populatesMdcKeys() {
        LogContext.set("abc123", "span456");

        assertThat(MDC.get(LogContext.KEY_TRACE_ID)).isEqualTo("abc123");
        assertThat(MDC.get(LogContext.KEY_SPAN_ID)).isEqualTo("span456");
    }

    @Test
    void clear_removesMdcKeys() {
        LogContext.set("abc123", "span456");
        LogContext.clear();

        assertThat(MDC.get(LogContext.KEY_TRACE_ID)).isNull();
        assertThat(MDC.get(LogContext.KEY_SPAN_ID)).isNull();
    }

    @Test
    void set_overwritesPreviousValues() {
        LogContext.set("trace1", "span1");
        LogContext.set("trace2", "span2");

        assertThat(MDC.get(LogContext.KEY_TRACE_ID)).isEqualTo("trace2");
        assertThat(MDC.get(LogContext.KEY_SPAN_ID)).isEqualTo("span2");
    }

    @Test
    void clear_isIdempotent() {
        LogContext.clear();
        LogContext.clear();

        assertThat(MDC.get(LogContext.KEY_TRACE_ID)).isNull();
        assertThat(MDC.get(LogContext.KEY_SPAN_ID)).isNull();
    }
}
