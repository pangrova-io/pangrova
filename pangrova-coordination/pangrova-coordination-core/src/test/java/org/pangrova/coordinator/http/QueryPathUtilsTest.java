package org.pangrova.coordinator.http;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link QueryPathUtils}.
 */
class QueryPathUtilsTest {

    @Test
    void extractsQueryIdBeforeStateSegment() {
        String queryId = QueryPathUtils.extractQueryId("/v1/query/abc-123/state", "state");
        assertThat(queryId).isEqualTo("abc-123");
    }

    @Test
    void extractsQueryIdBeforeCancelSegment() {
        String queryId = QueryPathUtils.extractQueryId("/v1/query/q-42/cancel", "cancel");
        assertThat(queryId).isEqualTo("q-42");
    }

    @Test
    void returnsNullWhenActionSegmentNotFound() {
        String queryId = QueryPathUtils.extractQueryId("/v1/query/abc-123/status", "cancel");
        assertThat(queryId).isNull();
    }

    @Test
    void returnsNullWhenActionSegmentIsFirst() {
        String queryId = QueryPathUtils.extractQueryId("/state", "state");
        assertThat(queryId).isNull();
    }

    @Test
    void throwsOnNullPathInfo() {
        assertThatThrownBy(() -> QueryPathUtils.extractQueryId(null, "state"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsOnBlankPathInfo() {
        assertThatThrownBy(() -> QueryPathUtils.extractQueryId("  ", "state"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsOnNullActionSegment() {
        assertThatThrownBy(() -> QueryPathUtils.extractQueryId("/v1/query/abc/state", null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
