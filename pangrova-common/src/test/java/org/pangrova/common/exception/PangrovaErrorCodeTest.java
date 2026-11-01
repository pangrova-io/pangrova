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
        assertThat(PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED.getCode())
                .isEqualTo("AI-3017");
        assertThat(PangrovaErrorCode.AI_EMBEDDING_MODEL_DROP_UNAVAILABLE.getCode())
                .isEqualTo("AI-0007");
        assertThat(PangrovaErrorCode.MATERIALIZATION_OBJECT_NOT_FOUND.getCode())
                .isEqualTo("MATERIALIZATION-0001");
        assertThat(PangrovaErrorCode.MATERIALIZATION_REQUEST_REJECTED.getCode())
                .isEqualTo("MATERIALIZATION-0016");
        assertThat(PangrovaErrorCode.METADATA_FILE_SPACE_NOT_FOUND.getCode())
                .isEqualTo("METADATA-0019");
        assertThat(PangrovaErrorCode.METADATA_FILE_SPACE_ALREADY_EXISTS.getCode())
                .isEqualTo("METADATA-0020");
        assertThat(PangrovaErrorCode.FILE_OBJECT_NOT_FOUND.getCode()).isEqualTo("FILE-0001");
        assertThat(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED.getCode())
                .isEqualTo("FILE-0002");
        assertThat(PangrovaErrorCode.FILE_PATH_INVALID.getCode()).isEqualTo("FILE-0003");
        assertThat(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID.getCode()).isEqualTo("FILE-0004");
        assertThat(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE.getCode()).isEqualTo("FILE-0005");
        assertThat(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED.getCode())
                .isEqualTo("FILE-0006");
        assertThat(PangrovaErrorCode.FILE_LIMIT_EXCEEDED.getCode()).isEqualTo("FILE-0007");
    }

    @Test
    void enumHasExpectedSize() {
        assertThat(PangrovaErrorCode.values()).hasSize(195);
    }
}
