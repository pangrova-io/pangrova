package org.pangrova.common.http;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link HttpResponseCheck}.
 * Covers success (2xx), client error (4xx), server error (5xx),
 * boundary values, and private constructor inaccessibility.
 *
 * <p>Validates: Requirements 12.1-12.3
 */
class HttpResponseCheckTest {

    // ── isSuccess (Requirement 12.2) ──

    @Test
    void isSuccess_200_returnsTrue() {
        assertThat(HttpResponseCheck.isSuccess(200)).isTrue();
    }

    @Test
    void isSuccess_201_returnsTrue() {
        assertThat(HttpResponseCheck.isSuccess(201)).isTrue();
    }

    @Test
    void isSuccess_299_returnsTrue() {
        assertThat(HttpResponseCheck.isSuccess(299)).isTrue();
    }

    @Test
    void isSuccess_300_returnsFalse() {
        assertThat(HttpResponseCheck.isSuccess(300)).isFalse();
    }

    @Test
    void isSuccess_199_returnsFalse() {
        assertThat(HttpResponseCheck.isSuccess(199)).isFalse();
    }

    // ── isClientError (Requirement 12.3) ──

    @Test
    void isClientError_400_returnsTrue() {
        assertThat(HttpResponseCheck.isClientError(400)).isTrue();
    }

    @Test
    void isClientError_404_returnsTrue() {
        assertThat(HttpResponseCheck.isClientError(404)).isTrue();
    }

    @Test
    void isClientError_499_returnsTrue() {
        assertThat(HttpResponseCheck.isClientError(499)).isTrue();
    }

    @Test
    void isClientError_500_returnsFalse() {
        assertThat(HttpResponseCheck.isClientError(500)).isFalse();
    }

    @Test
    void isClientError_399_returnsFalse() {
        assertThat(HttpResponseCheck.isClientError(399)).isFalse();
    }

    // ── isServerError (Requirement 12.3) ──

    @Test
    void isServerError_500_returnsTrue() {
        assertThat(HttpResponseCheck.isServerError(500)).isTrue();
    }

    @Test
    void isServerError_503_returnsTrue() {
        assertThat(HttpResponseCheck.isServerError(503)).isTrue();
    }

    @Test
    void isServerError_599_returnsTrue() {
        assertThat(HttpResponseCheck.isServerError(599)).isTrue();
    }

    @Test
    void isServerError_499_returnsFalse() {
        assertThat(HttpResponseCheck.isServerError(499)).isFalse();
    }

    // ── 199: none of the above ──

    @Test
    void statusCode199_isNeitherSuccessNorClientErrorNorServerError() {
        assertThat(HttpResponseCheck.isSuccess(199)).isFalse();
        assertThat(HttpResponseCheck.isClientError(199)).isFalse();
        assertThat(HttpResponseCheck.isServerError(199)).isFalse();
    }

    // ── Private constructor inaccessibility ──

    @Test
    void cannotInstantiate() throws Exception {
        var ctor = HttpResponseCheck.class.getDeclaredConstructor();
        assertThat(ctor.canAccess(null)).isFalse();
    }
}
