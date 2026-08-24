package org.pangrova.common.exception;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PangrovaErrorCodeTest {

    @Test
    void allCodesMatchCategoryFormat() {
        for (PangrovaErrorCode code : PangrovaErrorCode.values()) {
            assertThat(code.getCode()).matches("^[A-Z]+-\\d{4}$");
            assertThat(code.getDescription()).isNotBlank();
        }
    }

    @Test
    void expectedCodeValues() {
        assertThat(PangrovaErrorCode.CONFIG_MISSING_KEY.getCode()).isEqualTo("CONFIG-0001");
        assertThat(PangrovaErrorCode.CONFIG_INVALID_VALUE.getCode()).isEqualTo("CONFIG-0002");
        assertThat(PangrovaErrorCode.VALIDATION_NULL_ARGUMENT.getCode()).isEqualTo("VALIDATION-0001");
        assertThat(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT.getCode()).isEqualTo("VALIDATION-0002");
        assertThat(PangrovaErrorCode.SPI_PROVIDER_NOT_FOUND.getCode()).isEqualTo("SPI-0001");
        assertThat(PangrovaErrorCode.SPI_MULTIPLE_PROVIDERS.getCode()).isEqualTo("SPI-0002");
        assertThat(PangrovaErrorCode.PROTOCOL_SERIALIZATION_FAILED.getCode()).isEqualTo("PROTOCOL-0001");
        assertThat(PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED.getCode()).isEqualTo("PROTOCOL-0002");
    }

    @Test
    void enumHasExpectedSize() {
        assertThat(PangrovaErrorCode.values()).hasSize(163);
    }
}
