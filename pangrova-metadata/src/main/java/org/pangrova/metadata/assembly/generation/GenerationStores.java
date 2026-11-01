package org.pangrova.metadata.assembly.generation;

import org.pangrova.persistence.api.generation.transaction.jdbc.GenerationTransactionAuthority;
import org.pangrova.persistence.api.generation.transaction.jdbc.GenerationTransactionInputs;
import org.pangrova.persistence.jdbc.generation.query.PostgresGenerationQueries;
import org.pangrova.persistence.jdbc.generation.execution.PostgresGenerationExecutionFence;
import org.pangrova.persistence.api.generation.transaction.jdbc.GenerationExecutionTransactionFence;
import org.pangrova.persistence.jdbc.query.coordination.JdbcExecutionTransactionFence;
import org.pangrova.metadata.adapter.generation.MetadataGenerationTransactionAuthority;
import org.pangrova.metadata.adapter.generation.MetadataGenerationPublicationCandidates;
import org.pangrova.persistence.api.generation.transaction.jdbc.GenerationPublicationCandidates;
import org.pangrova.persistence.api.security.policy.CurrentDirectRoleReader;
import org.pangrova.security.spi.jdbc.CurrentModelAuthority;
import org.pangrova.persistence.jdbc.security.policy.JdbcCurrentDirectRoleReader;

import org.pangrova.persistence.jdbc.generation.job.PostgresGenerationJobStore;
import org.pangrova.persistence.jdbc.generation.job.PostgresGenerationStageExecutionStore;
import org.pangrova.persistence.api.generation.execution.GenerationStageExecutionStore;
import org.pangrova.persistence.api.generation.exchange.GenerationStageExchangeStore;
import org.pangrova.persistence.jdbc.generation.exchange.PostgresGenerationStageExchangeStore;
import org.pangrova.metadata.generation.jdbc.JdbcIndexGenerationPublicationStore;
import org.pangrova.metadata.generation.jdbc.JdbcGeneratedValuePublicationStore;
import org.pangrova.metadata.generation.jdbc.JdbcTabularExportPublicationStore;
import org.pangrova.metadata.generation.jdbc.submission.JdbcGeneratedValueBuildSubmissionStore;

import javax.sql.DataSource;

/** Composes artifact stores with the current tenant/table/model authority of the metadata database. */
public final class GenerationStores {
    private final GenerationTransactionAuthority authority;
    private final GenerationTransactionInputs inputs;

    private static final GenerationExecutionTransactionFence EXECUTION_FENCE =
            new PostgresGenerationExecutionFence(new JdbcExecutionTransactionFence());

    private static final CurrentDirectRoleReader CURRENT_ROLES = new JdbcCurrentDirectRoleReader();
    private static final GenerationPublicationCandidates PUBLICATION_CANDIDATES =
            new MetadataGenerationPublicationCandidates();

    /** Composes Metadata tenant/table checks with the supplied live model authority. */
    public GenerationStores(CurrentModelAuthority modelAuthority) {
        inputs = new PostgresGenerationQueries();
        authority = new MetadataGenerationTransactionAuthority(inputs, modelAuthority);
    }

    /** Current Security-owned direct roles on the caller connection. */
    public static CurrentDirectRoleReader currentRoles() { return CURRENT_ROLES; }

    /** Shared-database publication candidate projection, never an execution owner authority. */
    public static GenerationPublicationCandidates publicationCandidates() { return PUBLICATION_CANDIDATES; }

    /** Artifact routing composed with the real Query transaction fence. */
    public static GenerationExecutionTransactionFence executionFence() { return EXECUTION_FENCE; }

    /** Same-connection authority for caller-owned cross-domain transactions. */
    public GenerationTransactionAuthority authority() { return authority; }

    public PostgresGenerationJobStore builds(DataSource dataSource) {
        return new PostgresGenerationJobStore(
                dataSource, authority, EXECUTION_FENCE, CURRENT_ROLES, PUBLICATION_CANDIDATES);
    }

    public GenerationStageExecutionStore stages(DataSource dataSource) {
        return new PostgresGenerationStageExecutionStore(dataSource, authority, EXECUTION_FENCE, CURRENT_ROLES);
    }

    public GenerationStageExchangeStore exchanges(DataSource dataSource) {
        return new PostgresGenerationStageExchangeStore(dataSource, authority, EXECUTION_FENCE, CURRENT_ROLES);
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
