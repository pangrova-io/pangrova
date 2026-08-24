package org.pangrova.coordinator.http.endpoint.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Request body for POST /v1/namespaces/{namespace}/views/{view} — commits
 * changes to a view using the Iceberg REST catalog view commit protocol.
 *
 * <p>View commits follow the same CAS pattern as table commits: requirements
 * assert current state, updates describe mutations. The server validates all
 * requirements before applying any updates.</p>
 *
 * @param requirements assertions about the current view state
 * @param updates      mutations to apply if all requirements hold
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CommitViewRequest(
        List<ViewRequirement> requirements,
        List<ViewUpdate> updates) {

    public CommitViewRequest {
        requirements = requirements == null ? List.of() : List.copyOf(requirements);
        updates = updates == null ? List.of() : List.copyOf(updates);
    }

    /**
     * A view requirement (assertion about current state). The {@code type} field
     * discriminates which assertion to evaluate.
     *
     * <p>Supported types:
     * <ul>
     *   <li>{@code assert-view-uuid} — current view UUID must match {@code uuid}</li>
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ViewRequirement(
            String type,
            String uuid) {
    }

    /**
     * A view update (mutation to apply). The {@code action} field discriminates
     * the update type.
     *
     * <p>Supported actions:
     * <ul>
     *   <li>{@code assign-uuid} — assigns a UUID to the view</li>
     *   <li>{@code upgrade-format-version} — upgrades the view format version</li>
     *   <li>{@code set-location} — sets the view metadata location</li>
     *   <li>{@code set-properties} — adds or updates view properties</li>
     *   <li>{@code remove-properties} — removes view properties by key</li>
     *   <li>{@code add-view-version} — adds a new view version with SQL definition</li>
     *   <li>{@code set-current-view-version} — sets the current view version ID</li>
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ViewUpdate(
            String action,
            String uuid,
            @JsonProperty("format-version") Integer formatVersion,
            String location,
            Map<String, String> updates,
            List<String> removals,
            @JsonProperty("view-version") ViewVersion viewVersion,
            @JsonProperty("view-version-id") Integer viewVersionId) {

        public ViewUpdate {
            updates = updates == null ? null : Map.copyOf(updates);
            removals = removals == null ? null : List.copyOf(removals);
        }
    }

    /**
     * A view version definition containing the SQL representation and schema.
     *
     * @param versionId        version identifier
     * @param schemaId         schema this version references
     * @param timestampMs      creation timestamp in milliseconds
     * @param summary          version metadata summary
     * @param representations  list of SQL representations (dialect-specific)
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ViewVersion(
            @JsonProperty("version-id") int versionId,
            @JsonProperty("schema-id") int schemaId,
            @JsonProperty("timestamp-ms") long timestampMs,
            Map<String, String> summary,
            List<ViewRepresentation> representations) {

        public ViewVersion {
            summary = summary == null ? Map.of() : Map.copyOf(summary);
            representations = representations == null ? List.of() : List.copyOf(representations);
        }
    }

    /**
     * A view representation — a SQL query string plus the dialect it targets.
     *
     * @param type    representation type, typically "sql"
     * @param sql     the SQL query defining the view
     * @param dialect the SQL dialect (e.g., "pangrova", "spark", "trino")
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ViewRepresentation(
            String type,
            String sql,
            String dialect) {
    }
}
