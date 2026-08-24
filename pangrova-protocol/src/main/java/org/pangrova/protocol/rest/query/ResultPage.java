package org.pangrova.protocol.rest.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single page of query results returned by the paginated fetch API.
 * Contains Arrow IPC data for the current page and an optional token
 * to fetch the next page.
 *
 * <p>When {@code nextToken} is null, the result set is exhausted.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ResultPage(
    @JsonProperty("data") byte[] data,
    @JsonProperty("next_token") String nextToken,
    @JsonProperty("row_count") int rowCount
) {

    public ResultPage {
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative");
        }
    }

    /**
     * Returns true if there are more pages available.
     */
    public boolean hasMore() {
        return nextToken != null && !nextToken.isEmpty();
    }
}
