package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response body for a split pull request.
 * Contains the assigned split data or indicates no more splits are available.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SplitResponse(
        @JsonProperty("split_data") String splitData,
        @JsonProperty("split_index") int splitIndex,
        @JsonProperty("has_more") boolean hasMore,
        @JsonProperty("data_locations") List<String> dataLocations
) {

    /** Sentinel response when no more splits are available. */
    public static final SplitResponse NO_MORE_SPLITS =
            new SplitResponse(null, -1, false, List.of());

    public SplitResponse {
        dataLocations = dataLocations == null ? List.of() : List.copyOf(dataLocations);
    }

    public boolean hasSplit() {
        return splitData != null;
    }
}
