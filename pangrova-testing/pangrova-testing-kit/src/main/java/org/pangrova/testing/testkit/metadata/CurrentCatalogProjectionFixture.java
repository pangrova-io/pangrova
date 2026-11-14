package org.pangrova.testing.testkit.metadata;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Seeds fixture data through the real atomic port with the fixture's current deployment.
 * This is not a source-handle writer: stale-handle tests must submit their captured identity
 * directly to MetadataProjectionTransactions. Both dependencies remain borrowed.
 */
public final class CurrentCatalogProjectionFixture {
    private final CatalogDeploymentRepository deployments;
    private final MetadataProjectionTransactions transactions;

    public CurrentCatalogProjectionFixture(CatalogDeploymentRepository deployments,
            MetadataProjectionTransactions transactions) {
        this.deployments = Objects.requireNonNull(deployments, "deployments");
        this.transactions = Objects.requireNonNull(transactions, "transactions");
    }

    public MetadataProjectionReceipt saveTable(TableMetadata table) {
        var scope = identity(table.catalog());
        return transactions.apply(transactions.beginObservation(scope), new MetadataProjectionMutation.UpsertTable(
                UUID.randomUUID(), scope, table));
    }

    public MetadataProjectionReceipt deleteTable(String catalog, String schema, String table) {
        var scope = identity(catalog);
        return transactions.apply(transactions.beginObservation(scope), new MetadataProjectionMutation.DeleteTable(UUID.randomUUID(), scope,
                new QualifiedTableRef(catalog, schema, table)));
    }

    public MetadataProjectionReceipt saveSchemas(String catalog, List<String> schemas) {
        var scope = identity(catalog);
        return transactions.apply(transactions.beginObservation(scope), new MetadataProjectionMutation.ObserveSchemas(
                UUID.randomUUID(), scope, schemas));
    }

    private CatalogDeploymentIdentity identity(String catalog) {
        return deployments.getCatalogDescriptor(catalog).orElseThrow(
                () -> new IllegalStateException("Fixture catalog has not been created: " + catalog)).identity();
    }
}
