package org.pangrova.coordinator.http.endpoint.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Request body for POST /v1/namespaces/{namespace}/tables/{table} — updates a table
 * using the Iceberg REST catalog commit protocol.
 *
 * <p>The commit protocol is a compare-and-swap (CAS): the client sends a list of
 * {@code requirements} (assertions about the current table state) and a list of
 * {@code updates} (mutations to apply). The server validates all requirements
 * before applying any updates atomically. If a requirement fails, the server
 * returns 409 Conflict — no updates are applied.</p>
 *
 * @param requirements assertions about current table state that must all hold
 * @param updates      mutations to apply atomically if all requirements pass
 */
public record UpdateTableRequest(
        List<Requirement> requirements,
        List<Update> updates) {

    public UpdateTableRequest {
        requirements = requirements == null ? List.of() : List.copyOf(requirements);
        updates = updates == null ? List.of() : List.copyOf(updates);
    }

    /**
     * A table requirement (assertion about current state). The {@code type} field
     * discriminates which assertion to evaluate; only the fields relevant to that
     * type are non-null.
     *
     * <p>Supported types per the Iceberg REST spec:
     * <ul>
     *   <li>{@code assert-create} — table must not already exist</li>
     *   <li>{@code assert-table-uuid} — current table UUID must match {@code uuid}</li>
     *   <li>{@code assert-ref-snapshot-id} — snapshot ID for ref {@code ref} must match</li>
     *   <li>{@code assert-last-assigned-field-id} — last assigned field ID must match</li>
     *   <li>{@code assert-current-schema-id} — current schema ID must match</li>
     *   <li>{@code assert-default-spec-id} — default partition spec ID must match</li>
     *   <li>{@code assert-default-sort-order-id} — default sort order ID must match</li>
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Requirement(
            String type,
            String uuid,
            String ref,
            @JsonProperty("snapshot-id") Long snapshotId,
            @JsonProperty("last-assigned-field-id") Integer lastAssignedFieldId,
            @JsonProperty("current-schema-id") Integer currentSchemaId,
            @JsonProperty("default-spec-id") Integer defaultSpecId,
            @JsonProperty("default-sort-order-id") Integer defaultSortOrderId) {
    }

    /**
     * A table update (mutation to apply). The {@code action} field discriminates
     * the update type; only the fields relevant to that action are non-null.
     *
     * <p>Supported actions per the Iceberg REST spec:
     * <ul>
     *   <li>{@code assign-uuid} — assigns a UUID to the table</li>
     *   <li>{@code upgrade-format-version} — upgrades the table format version</li>
     *   <li>{@code set-current-schema} — sets the current schema ID</li>
     *   <li>{@code set-default-spec} — sets the default partition spec ID</li>
     *   <li>{@code set-default-sort-order} — sets the default sort order ID</li>
     *   <li>{@code set-location} — sets the table location</li>
     *   <li>{@code set-properties} — adds or updates table properties</li>
     *   <li>{@code remove-properties} — removes table properties by key</li>
     *   <li>{@code add-snapshot} — adds a new snapshot to the table</li>
     *   <li>{@code set-snapshot-ref} — sets a snapshot reference (branch or tag)</li>
     *   <li>{@code remove-snapshots} — removes snapshots by ID</li>
     *   <li>{@code remove-snapshot-ref} — removes a snapshot reference</li>
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Update(
            String action,
            String uuid,
            @JsonProperty("format-version") Integer formatVersion,
            @JsonProperty("schema-id") Integer schemaId,
            @JsonProperty("spec-id") Integer specId,
            @JsonProperty("sort-order-id") Integer sortOrderId,
            String location,
            Map<String, String> updates,
            List<String> removals,
            @JsonProperty("ref-name") String refName,
            @JsonProperty("snapshot-id") Long snapshotId,
            @JsonProperty("snapshot-ids") List<Long> snapshotIds,
            String type) {

        public Update {
            updates = updates == null ? null : Map.copyOf(updates);
            removals = removals == null ? null : List.copyOf(removals);
            snapshotIds = snapshotIds == null ? null : List.copyOf(snapshotIds);
        }
    }
}
