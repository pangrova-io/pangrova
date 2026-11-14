package org.pangrova.common.resilience.policy;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.resilience.policy.ErrorClassifier.ErrorCategory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests ErrorClassifier transient vs permanent classification.
 * Validates Requirement 19.4: transient errors → retry, permanent → fail immediately.
 */
class ErrorClassifierTest {

    private final ErrorClassifier classifier = new ErrorClassifier();

    @Test
    void userError_notRetryable() {
        ErrorCategory category = classifier.classify(PangrovaErrorCode.VALIDATION_NULL_ARGUMENT);
        assertThat(category).isEqualTo(ErrorCategory.USER_ERROR);
        assertThat(classifier.isRetryable(category)).isFalse();
    }

    @Test
    void resourceError_retryable() {
        ErrorCategory category = classifier.classify(PangrovaErrorCode.RESOURCE_MEMORY_EXCEEDED);
        assertThat(category).isEqualTo(ErrorCategory.RESOURCE_ERROR);
        assertThat(classifier.isRetryable(category)).isTrue();
        assertThat(classifier.maxRetries(category)).isEqualTo(1);
    }

    @Test
    void externalError_retryable() {
        ErrorCategory category = classifier.classify(PangrovaErrorCode.METADATA_CATALOG_BACKEND_ERROR);
        assertThat(category).isEqualTo(ErrorCategory.EXTERNAL_ERROR);
        assertThat(classifier.isRetryable(category)).isTrue();
        assertThat(classifier.maxRetries(category)).isEqualTo(3);
    }

    @Test
    void exchangeError_retryable() {
        ErrorCategory category = classifier.classify(PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        assertThat(category).isEqualTo(ErrorCategory.EXCHANGE_ERROR);
        assertThat(classifier.isRetryable(category)).isTrue();
        assertThat(classifier.maxRetries(category)).isEqualTo(3);
    }

    @Test
    void internalError_notRetryable() {
        ErrorCategory category = classifier.classify(PangrovaErrorCode.CONFIG_MISSING_KEY);
        assertThat(category).isEqualTo(ErrorCategory.INTERNAL_ERROR);
        assertThat(classifier.isRetryable(category)).isFalse();
    }

    @Test
    void transportError_classifiedAsExchange() {
        ErrorCategory category = classifier.classify(PangrovaErrorCode.TRANSPORT_CONNECTION_FAILED);
        assertThat(category).isEqualTo(ErrorCategory.EXCHANGE_ERROR);
        assertThat(classifier.isRetryable(category)).isTrue();
    }

    @Test
    void nativeError_classifiedAsExternal() {
        ErrorCategory category = classifier.classify(PangrovaErrorCode.NATIVE_JNI_EXCEPTION);
        assertThat(category).isEqualTo(ErrorCategory.EXTERNAL_ERROR);
        assertThat(classifier.isRetryable(category)).isTrue();
    }
}
