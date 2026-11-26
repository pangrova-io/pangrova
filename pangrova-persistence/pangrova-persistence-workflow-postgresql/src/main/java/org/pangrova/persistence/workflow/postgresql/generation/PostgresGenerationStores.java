package org.pangrova.persistence.workflow.postgresql.generation;

import org.pangrova.persistence.workflow.api.security.TableControlWriteAuthority;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionInputs;
import org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries;
import org.pangrova.persistence.generation.postgresql.execution.PostgresGenerationExecutionFence;
import org.pangrova.persistence.jdbc.api.generation.GenerationExecutionTransactionFence;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionTransactionFence;
import org.pangrova.persistence.jdbc.api.generation.GenerationPublicationCandidates;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;
import org.pangrova.persistence.security.api.tenant.DeferredTenantAuthority;
import org.pangrova.persistence.security.postgresql.tenant.PostgresDeferredTenantAuthority;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;

import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationStageExecutionStore;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore;
import org.pangrova.persistence.generation.api.exchange.GenerationStageExchangeStore;
import org.pangrova.persistence.generation.postgresql.exchange.PostgresGenerationStageExchangeStore;
import org.pangrova.persistence.generation.postgresql.publication.JdbcIndexGenerationPublicationStore;
import org.pangrova.persistence.generation.postgresql.publication.JdbcGeneratedValuePublicationStore;
import org.pangrova.persistence.generation.postgresql.publication.JdbcTabularExportPublicationStore;
import org.pangrova.persistence.generation.postgresql.publication.submission.JdbcGeneratedValueBuildSubmissionStore;

import javax.sql.DataSource;

/** Composes artifact stores with the current tenant/table/model authority of the metadata database. */
public final class PostgresGenerationStores {
    private final GenerationTransactionAuthority authority;
    private final GenerationTransactionInputs inputs;

    private static final GenerationExecutionTransactionFence EXECUTION_FENCE =
            new PostgresGenerationExecutionFence(new JdbcExecutionTransactionFence());

    private static final DeferredTenantAuthority TENANT_AUTHORITY = new PostgresDeferredTenantAuthority();
    private static final GenerationPublicationCandidates PUBLICATION_CANDIDATES =
            new PostgresGenerationPublicationCandidates();

    /** Composes Metadata tenant/table checks with the supplied live model authority. */
    public PostgresGenerationStores(CurrentModelAuthority modelAuthority, TableControlWriteAuthority tableAuthority) {
        inputs = new PostgresGenerationQueries();
        authority = new PostgresGenerationTransactionAuthority(inputs, modelAuthority,
                TENANT_AUTHORITY, AuthorizationTransactionGate::lockPublication, tableAuthority);
    }

    /** Shared-database publication candidate projection, never an execution owner authority. */
    public static GenerationPublicationCandidates publicationCandidates() { return PUBLICATION_CANDIDATES; }

    /** Artifact routing composed with the real Query transaction fence. */
    public static GenerationExecutionTransactionFence executionFence() { return EXECUTION_FENCE; }

    /** Same-connection authority for caller-owned cross-domain transactions. */
    public GenerationTransactionAuthority authority() { return authority; }

    public PostgresGenerationJobStore builds(DataSource dataSource) {
        return new PostgresGenerationJobStore(
                dataSource, authority, EXECUTION_FENCE, PUBLICATION_CANDIDATES);
    }

    public GenerationStageExecutionStore stages(DataSource dataSource) {
        return new PostgresGenerationStageExecutionStore(dataSource, authority, EXECUTION_FENCE);
    }

    public GenerationStageExchangeStore exchanges(DataSource dataSource) {
        return new PostgresGenerationStageExchangeStore(dataSource, authority, EXECUTION_FENCE);
    }

    public JdbcIndexGenerationPublicationStore indexPublications(DataSource dataSource) {
        return new JdbcIndexGenerationPublicationStore(dataSource, authority, EXECUTION_FENCE, inputs);
    }

    public JdbcGeneratedValuePublicationStore generatedPublications(DataSource dataSource) {
        return new JdbcGeneratedValuePublicationStore(dataSource, authority, EXECUTION_FENCE, inputs);
    }

    public JdbcTabularExportPublicationStore tabularPublications(DataSource dataSource) {
        return new JdbcTabularExportPublicationStore(dataSource, authority, EXECUTION_FENCE, inputs);
    }

    public JdbcGeneratedValueBuildSubmissionStore generatedSubmissions(DataSource dataSource) {
        return new JdbcGeneratedValueBuildSubmissionStore(dataSource, authority, builds(dataSource));
    }
}
