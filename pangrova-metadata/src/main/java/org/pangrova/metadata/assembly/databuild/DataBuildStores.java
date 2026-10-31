package org.pangrova.metadata.assembly.databuild;

import org.pangrova.databuild.spi.jdbc.DataBuildTransactionAuthority;
import org.pangrova.databuild.store.jdbc.execution.JdbcDataBuildExecutionFence;
import org.pangrova.query.store.jdbc.anchor.JdbcExecutionTransactionFence;
import org.pangrova.metadata.adapter.databuild.MetadataDataBuildTransactionAuthority;
import org.pangrova.metadata.adapter.databuild.MetadataDataBuildPublicationCandidates;
import org.pangrova.databuild.spi.jdbc.DataBuildPublicationCandidates;
import org.pangrova.security.spi.jdbc.CurrentDirectRoleReader;
import org.pangrova.security.spi.jdbc.CurrentModelAuthority;
import org.pangrova.security.store.jdbc.JdbcCurrentDirectRoleReader;

import org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStore;
import org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStageExecutionStore;
import org.pangrova.databuild.spi.storage.DataBuildStageExecutionStore;
import org.pangrova.databuild.spi.storage.DataBuildStageExchangeStore;
import org.pangrova.databuild.store.jdbc.exchange.JdbcDataBuildStageExchangeStore;
import org.pangrova.metadata.databuild.jdbc.JdbcIndexGenerationPublicationStore;
import org.pangrova.metadata.databuild.jdbc.JdbcGeneratedValuePublicationStore;
import org.pangrova.metadata.databuild.jdbc.JdbcTabularExportPublicationStore;
import org.pangrova.metadata.databuild.jdbc.submission.JdbcGeneratedValueBuildSubmissionStore;

import javax.sql.DataSource;

/** Composes artifact stores with the current tenant/table/model authority of the metadata database. */
public final class DataBuildStores {
    private final DataBuildTransactionAuthority authority;

    private static final JdbcDataBuildExecutionFence EXECUTION_FENCE =
            new JdbcDataBuildExecutionFence(new JdbcExecutionTransactionFence());

    private static final CurrentDirectRoleReader CURRENT_ROLES = new JdbcCurrentDirectRoleReader();
    private static final DataBuildPublicationCandidates PUBLICATION_CANDIDATES =
            new MetadataDataBuildPublicationCandidates();

    /** Composes Metadata tenant/table checks with the supplied live model authority. */
    public DataBuildStores(CurrentModelAuthority modelAuthority) {
        authority = new MetadataDataBuildTransactionAuthority(modelAuthority);
    }

    /** Current Security-owned direct roles on the caller connection. */
    public static CurrentDirectRoleReader currentRoles() { return CURRENT_ROLES; }

    /** Shared-database publication candidate projection, never an execution owner authority. */
    public static DataBuildPublicationCandidates publicationCandidates() { return PUBLICATION_CANDIDATES; }

    /** Artifact routing composed with the real Query transaction fence. */
    public static JdbcDataBuildExecutionFence executionFence() { return EXECUTION_FENCE; }

    /** Same-connection authority for caller-owned cross-domain transactions. */
    public DataBuildTransactionAuthority authority() { return authority; }

    public JdbcDataBuildStore builds(DataSource dataSource) {
        return new JdbcDataBuildStore(
                dataSource, authority, EXECUTION_FENCE, CURRENT_ROLES, PUBLICATION_CANDIDATES);
    }

    public DataBuildStageExecutionStore stages(DataSource dataSource) {
        return new JdbcDataBuildStageExecutionStore(dataSource, authority, EXECUTION_FENCE, CURRENT_ROLES);
    }

    public DataBuildStageExchangeStore exchanges(DataSource dataSource) {
        return new JdbcDataBuildStageExchangeStore(dataSource, authority, EXECUTION_FENCE, CURRENT_ROLES);
    }

    public JdbcIndexGenerationPublicationStore indexPublications(DataSource dataSource) {
        return new JdbcIndexGenerationPublicationStore(dataSource, authority, EXECUTION_FENCE);
    }

    public JdbcGeneratedValuePublicationStore generatedPublications(DataSource dataSource) {
        return new JdbcGeneratedValuePublicationStore(dataSource, authority, EXECUTION_FENCE);
    }

    public JdbcTabularExportPublicationStore tabularPublications(DataSource dataSource) {
        return new JdbcTabularExportPublicationStore(dataSource, authority, EXECUTION_FENCE);
    }

    public JdbcGeneratedValueBuildSubmissionStore generatedSubmissions(DataSource dataSource) {
        return new JdbcGeneratedValueBuildSubmissionStore(dataSource, authority);
    }
}
