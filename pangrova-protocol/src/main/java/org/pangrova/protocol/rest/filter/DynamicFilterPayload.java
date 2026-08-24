package org.pangrova.protocol.rest.filter;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Coordinator-to-Worker dynamic filter payload for DPP (Dynamic Partition Pruning).
 *
 * <p>Carries a serialized runtime filter that the Worker applies to TableScan
 * before reading splits/pages, reducing probe-side I/O.</p>
 *
 * <h3>Version compatibility</h3>
 * <ul>
 *   <li>{@code @JsonIgnoreProperties(ignoreUnknown = true)} — unknown fields
 *       from newer senders are silently ignored during deserialization.</li>
 *   <li>{@code filterData} is Base64-encoded by Jackson automatically for
 *       {@code byte[]} fields.</li>
 *   <li>{@code version} defaults to {@code 0} when absent (Java primitive default),
 *       indicating "unversioned" payload.</li>
 * </ul>
 *
 * @param filterId      unique identifier for this filter instance (mandatory)
 * @param filterType    filter kind (e.g., "BLOOM", "MIN_MAX", "IN_LIST") (mandatory)
 * @param filterData    opaque serialized filter bytes (mandatory, may be empty)
 * @param targetStageId stage that should apply this filter (mandatory)
 * @param column        target column name for this filter (nullable for backward compat)
 * @param version       payload schema version for forward compatibility
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DynamicFilterPayload(
        @JsonProperty("filter_id") String filterId,
        @JsonProperty("filter_type") String filterType,
        @JsonProperty("filter_data") byte[] filterData,
        @JsonProperty("target_stage_id") String targetStageId,
        @JsonProperty("column") String column,
        @JsonProperty("version") int version
) {

    /**
     * Compact constructor enforcing mandatory fields and defensive copy.
     */
    public DynamicFilterPayload {
        if (filterId == null || filterId.isBlank()) {
            throw new IllegalArgumentException("filterId must not be null or blank");
        }
        if (filterType == null || filterType.isBlank()) {
            throw new IllegalArgumentException("filterType must not be null or blank");
        }
        if (filterData == null) {
            throw new IllegalArgumentException("filterData must not be null");
        }
        if (targetStageId == null || targetStageId.isBlank()) {
            throw new IllegalArgumentException("targetStageId must not be null or blank");
        }
        // column is nullable for backward compatibility
        filterData = filterData.clone();
    }

    /**
     * Backward-compatible constructor without column field.
     */
    public DynamicFilterPayload(String filterId, String filterType, byte[] filterData,
                                String targetStageId, int version) {
        this(filterId, filterType, filterData, targetStageId, null, version);
    }

    /**
     * Returns a defensive copy of the filter data bytes.
     */
    @Override
    public byte[] filterData() {
        return filterData.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DynamicFilterPayload that)) return false;
        return version == that.version
                && filterId.equals(that.filterId)
                && filterType.equals(that.filterType)
                && Arrays.equals(filterData, that.filterData)
                && targetStageId.equals(that.targetStageId)
                && java.util.Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        int result = filterId.hashCode();
        result = 31 * result + filterType.hashCode();
        result = 31 * result + Arrays.hashCode(filterData);
        result = 31 * result + targetStageId.hashCode();
        result = 31 * result + (column != null ? column.hashCode() : 0);
        result = 31 * result + version;
        return result;
    }

    @Override
    public String toString() {
        return "DynamicFilterPayload[filterId=" + filterId
                + ", filterType=" + filterType
                + ", filterDataLength=" + filterData.length
                + ", targetStageId=" + targetStageId
                + ", column=" + column
                + ", version=" + version + "]";
    }
}
