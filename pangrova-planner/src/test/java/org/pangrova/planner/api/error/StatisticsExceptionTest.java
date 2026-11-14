package org.pangrova.planner.api.error;

import org.pangrova.planner.api.exception.StatisticsException;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StatisticsExceptionTest {

    @Test
    void exceptionCarriesErrorCode() {
        var ex = new StatisticsException(
                PangrovaErrorCode.STATS_COLLECTION_TIMEOUT, "timed out");

        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.STATS_COLLECTION_TIMEOUT);
        assertThat(ex.getMessage()).isEqualTo("timed out");
    }

    @Test
    void exceptionCarriesCause() {
        var cause = new RuntimeException("io error");
        var ex = new StatisticsException(
                PangrovaErrorCode.STATS_COLLECTION_TIMEOUT, "timed out", cause);

        assertThat(ex.getCause()).isSameAs(cause);
    }
}
