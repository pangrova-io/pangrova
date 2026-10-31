package org.pangrova.ai.control.store.jdbc.feature;

import org.pangrova.ai.control.feature.CatalogExportBuildStore;
import org.pangrova.ai.control.feature.CatalogExportRequest;
import org.pangrova.generation.api.GenerationExecutionKind;
import org.pangrova.generation.api.GenerationOutputTypeIds;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationPage;
import org.pangrova.generation.spi.jdbc.GenerationJobSubmissionTransactions;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.ai.control.api.CatalogExportStorageException;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/** Owns the one transaction that inserts a generic build and its AI catalog export association. */
@Internal
public final class JdbcCatalogExportBuildStore implements CatalogExportBuildStore {
    private final DataSource dataSource;
    private final GenerationJobSubmissionTransactions jobSubmissions;

    public JdbcCatalogExportBuildStore(
            DataSource dataSource, GenerationJobSubmissionTransactions jobSubmissions) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.jobSubmissions = Objects.requireNonNull(jobSubmissions, "jobSubmissions");
    }

    @Override
    public void createCatalogExportJob(CatalogExportRequest request, GenerationJob job,
            GenerationDispatchSpec dispatchSpec, List<GenerationPage> pages) {
        requireIdentity(request, job, dispatchSpec);
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                jobSubmissions.createJob(connection, job, dispatchSpec, pages);
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

    private static void requireIdentity(CatalogExportRequest request, GenerationJob job,
            GenerationDispatchSpec dispatchSpec) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(dispatchSpec, "dispatchSpec");
        if (!request.tenantId().equals(job.tenantId()) || !request.tenantId().equals(dispatchSpec.tenantId())
                || !request.jobId().equals(job.jobId()) || !request.jobId().equals(dispatchSpec.jobId())
                || !request.requestedBy().equals(dispatchSpec.principal())
                || !request.artifactId().equals(job.artifactId())
                || !request.generation().equals(job.stagingGeneration())
                || !request.dependencySha256().equalsIgnoreCase(job.dependencySet().sha256())
                || !GenerationOutputTypeIds.TABULAR_EXPORT.equals(job.artifactType())
                || job.executionKind() != GenerationExecutionKind.EXPORT) {
            throw new IllegalArgumentException("Catalog export link differs from its authorized artifact job identity");
        }
    }
}
