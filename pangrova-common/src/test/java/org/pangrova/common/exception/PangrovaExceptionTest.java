package org.pangrova.common.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PangrovaExceptionTest {

    @Test
    void constructWithCodeAndMessage() {
        PangrovaException ex = new PangrovaException(
                PangrovaErrorCode.SPI_PROVIDER_NOT_FOUND, "no provider");

        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.SPI_PROVIDER_NOT_FOUND);
        assertThat(ex.getMessage()).isEqualTo("no provider");
        assertThat(ex.getCause()).isNull();
    }

    @Test
    void constructWithCodeMessageAndCause() {
        Throwable cause = new RuntimeException("root");
        PangrovaException ex = new PangrovaException(
                PangrovaErrorCode.SPI_MULTIPLE_PROVIDERS, "dup", cause);

        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.SPI_MULTIPLE_PROVIDERS);
        assertThat(ex.getMessage()).isEqualTo("dup");
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void isRuntimeException() {
        assertThat(new PangrovaException(PangrovaErrorCode.SPI_PROVIDER_NOT_FOUND, "x"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void configurationExceptionPreservesCode() {
        ConfigurationException ex = new ConfigurationException(
                PangrovaErrorCode.CONFIG_MISSING_KEY, "key missing");

        assertThat(ex).isInstanceOf(PangrovaException.class);
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.CONFIG_MISSING_KEY);
    }

    @Test
    void validationExceptionPreservesCode() {
        ValidationException ex = new ValidationException(
                PangrovaErrorCode.VALIDATION_NULL_ARGUMENT, "null arg");

        assertThat(ex).isInstanceOf(PangrovaException.class);
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.VALIDATION_NULL_ARGUMENT);
    }

    @Test
    void serializationExceptionPreservesCode() {
        Throwable cause = new RuntimeException("io");
        SerializationException ex = new SerializationException(
                PangrovaErrorCode.PROTOCOL_SERIALIZATION_FAILED, "ser fail", cause);

        assertThat(ex).isInstanceOf(PangrovaException.class);
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.PROTOCOL_SERIALIZATION_FAILED);
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void sourceUnavailableExceptionPreservesCode() {
        SourceUnavailableException ex = new SourceUnavailableException(
                PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE, "broker unreachable");

        assertThat(ex).isInstanceOf(PangrovaException.class);
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE);
        assertThat(ex.getMessage()).isEqualTo("broker unreachable");
    }

    @Test
    void sourceUnavailableExceptionPreservesCodeAndCause() {
        Throwable cause = new RuntimeException("connection refused");
        SourceUnavailableException ex = new SourceUnavailableException(
                PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE, "broker down", cause);

        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE);
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void streamNotFoundExceptionPreservesCode() {
        StreamNotFoundException ex = new StreamNotFoundException(
                PangrovaErrorCode.STREAM_NOT_FOUND, "stream my_stream not found");

        assertThat(ex).isInstanceOf(PangrovaException.class);
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.STREAM_NOT_FOUND);
        assertThat(ex.getMessage()).isEqualTo("stream my_stream not found");
    }

    @Test
    void streamAlreadyExistsExceptionPreservesCode() {
        StreamAlreadyExistsException ex = new StreamAlreadyExistsException(
                PangrovaErrorCode.STREAM_ALREADY_EXISTS, "stream my_stream already exists");

        assertThat(ex).isInstanceOf(PangrovaException.class);
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.STREAM_ALREADY_EXISTS);
        assertThat(ex.getMessage()).isEqualTo("stream my_stream already exists");
    }

    @Test
    void streamQuotaExceededExceptionPreservesCode() {
        StreamQuotaExceededException ex = new StreamQuotaExceededException(
                PangrovaErrorCode.STREAM_QUOTA_EXCEEDED, "tenant limit reached");

        assertThat(ex).isInstanceOf(PangrovaException.class);
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.STREAM_QUOTA_EXCEEDED);
        assertThat(ex.getMessage()).isEqualTo("tenant limit reached");
    }
}
