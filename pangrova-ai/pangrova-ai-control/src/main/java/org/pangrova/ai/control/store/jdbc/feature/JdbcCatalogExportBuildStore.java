package org.pangrova.ai.control.store.jdbc.feature;

import org.pangrova.ai.control.feature.CatalogExportBuildStore;
import org.pangrova.ai.control.feature.CatalogExportRequest;
import org.pangrova.databuild.api.DataBuildExecutionKind;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.spi.jdbc.DataBuildTransactionAuthority;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStore;
import org.pangrova.ai.control.api.CatalogExportStorageException;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/** Owns the one transaction that inserts a generic build and its AI catalog export association. */
@Internal
public final class JdbcCatalogExportBuildStore implements CatalogExportBuildStore {
    private final DataSource dataSource;
    private final DataBuildTransactionAuthority transactionAuthority;

    public JdbcCatalogExportBuildStore(DataSource dataSource, DataBuildTransactionAuthority transactionAuthority) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.transactionAuthority = Objects.requireNonNull(transactionAuthority, "transactionAuthority");
    }

    @Override
    public void createCatalogExportJob(CatalogExportRequest request, DataBuildJob job,
            DataBuildDispatchSpec dispatchSpec, List<DataBuildPage> pages) {
        requireIdentity(request, job, dispatchSpec);
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                JdbcDataBuildStore.createJobWithinTransaction(
                        transactionAuthority, connection, job, dispatchSpec, pages);
                JdbcCatalogExportJobInsertion.insert(connection, request);
                connection.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new CatalogExportStorageException("Failed to create catalog export artifact job", failure);
        }
    }

    private static void requireIdentity(CatalogExportRequest request, DataBuildJob job,
            DataBuildDispatchSpec dispatchSpec) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(dispatchSpec, "dispatchSpec");
        if (!request.tenantId().equals(job.tenantId()) || !request.tenantId().equals(dispatchSpec.tenantId())
                || !request.jobId().equals(job.jobId()) || !request.jobId().equals(dispatchSpec.jobId())
                || !request.requestedBy().equals(dispatchSpec.principal())
                || !request.artifactId().equals(job.artifactId())
                || !request.generation().equals(job.stagingGeneration())
                || !request.dependencySha256().equalsIgnoreCase(job.dependencySet().sha256())
                || !DataBuildOutputTypeIds.TABULAR_EXPORT.equals(job.artifactType())
                || job.executionKind() != DataBuildExecutionKind.EXPORT) {
            throw new IllegalArgumentException("Catalog export link differs from its authorized artifact job identity");
        }
    }
}
