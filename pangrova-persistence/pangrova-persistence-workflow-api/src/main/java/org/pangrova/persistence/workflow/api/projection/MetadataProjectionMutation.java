package org.pangrova.persistence.workflow.api.projection;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Immutable operation content. An operation ID may be replayed only with identical content. */
@Experimental
public sealed interface MetadataProjectionMutation {
    UUID operationId();
    CatalogDeploymentIdentity deployment();

    record UpsertTable(UUID operationId, CatalogDeploymentIdentity deployment, TableMetadata table)
            implements MetadataProjectionMutation {
        public UpsertTable {
            validate(operationId, deployment);
            Objects.requireNonNull(table, "table");
            requireCatalog(deployment, table.catalog());
        }
    }

    record DeleteTable(UUID operationId, CatalogDeploymentIdentity deployment, QualifiedTableRef table)
            implements MetadataProjectionMutation {
        public DeleteTable {
            validate(operationId, deployment);
            Objects.requireNonNull(table, "table");
            requireCatalog(deployment, table.catalog());
        }
    }

    /** Unordered observed names; absence does not authorize deletion of an existing schema. */
    record ObserveSchemas(UUID operationId, CatalogDeploymentIdentity deployment, List<String> schemas)
            implements MetadataProjectionMutation {
        public ObserveSchemas {
            validate(operationId, deployment);
            schemas = List.copyOf(new java.util.TreeSet<>(List.copyOf(schemas)));
        }
    }

    private static void validate(UUID operationId, CatalogDeploymentIdentity deployment) {
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(deployment, "deployment");
    }

    private static void requireCatalog(CatalogDeploymentIdentity deployment, String catalog) {
        if (!deployment.catalogName().equals(catalog)) {
            throw new MetadataProjectionScopeException(
                    "Projection target is outside the originating catalog deployment");
        }
    }
}
