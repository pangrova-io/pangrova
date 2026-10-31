package org.pangrova.coordinator.http.endpoint.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.TableIdentifier;

import java.util.List;

/**
 * Request body for POST /v1/transactions/commit — atomic multi-table commit.
 *
 * <p>Commits changes to multiple tables atomically using the Iceberg REST catalog
 * transaction protocol. All requirements across all table changes must be satisfied;
 * if any requirement fails, the entire transaction is rejected (all-or-nothing).</p>
 *
 * @param tableChanges list of per-table change sets, each containing an identifier,
 *                     requirements, and updates
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CommitTransactionRequest(
        @JsonProperty("table-changes") List<TableChange> tableChanges) {

    public CommitTransactionRequest {
        tableChanges = tableChanges == null ? List.of() : List.copyOf(tableChanges);
    }

    /**
     * A single table's change set within a multi-table transaction.
     *
     * @param identifier   the table to modify
     * @param requirements assertions about the table's current state
     * @param updates      mutations to apply if all requirements pass
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TableChange(
            TableIdentifier identifier,
            List<UpdateTableRequest.Requirement> requirements,
            List<UpdateTableRequest.Update> updates) {

        public TableChange {
            requirements = requirements == null ? List.of() : List.copyOf(requirements);
            updates = updates == null ? List.of() : List.copyOf(updates);
        }
    }
}
