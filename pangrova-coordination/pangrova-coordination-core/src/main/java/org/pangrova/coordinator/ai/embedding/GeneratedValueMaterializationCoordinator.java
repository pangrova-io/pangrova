package org.pangrova.coordinator.ai.embedding;

import org.pangrova.security.spi.jdbc.CurrentModelAuthority;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.protocol.metadata.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitListener;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.connector.coordination.WriteCommitEntry;
import org.pangrova.spi.connector.coordination.WriteCommitJournal;
import org.pangrova.spi.connector.coordination.WriteAuthorizationReceipt;
import org.pangrova.spi.connector.coordination.WriteDeclarationSnapshot;
import org.pangrova.spi.security.AuthorizationDeniedException;
import javax.sql.DataSource;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Control-plane bridge from table writes to durable generated-value builds.
 *
 * <p>SYNC definitions are rejected until the Native write relation carries
 * the generated expression in the same atomic catalog commit. ASYNC writes
 * are admitted only after exact model/materializer/format capability checks;
 * the connector's exact commit snapshot is then compiled into the shared
 * durable artifact state machine. No source row enters this component.</p>
 */
public final class GeneratedValueMaterializationCoordinator implements CommitListener {

    private final CatalogRegistry catalogRegistry;
    private final DurableGeneratedValueBuildService buildService;
    private final WriteCommitJournal writeJournal;
    private final GeneratedWriteAuthorityResolver writeAuthorityResolver;
    private volatile Set<String> registeredCatalogs = Set.of();

    /** Binds the original journal, catalog and model authority database to generated maintenance. */
    public GeneratedValueMaterializationCoordinator(
            CatalogRegistry catalogRegistry,
            DurableGeneratedValueBuildService buildService,
            DataSource dataSource, WriteCommitJournal writeJournal, CurrentModelAuthority modelAuthority) {
        this.catalogRegistry = Objects.requireNonNull(
                catalogRegistry, "catalogRegistry must not be null");
        this.buildService = Objects.requireNonNull(
                buildService, "buildService must not be null");
        this.writeJournal = Objects.requireNonNull(writeJournal, "writeJournal");
        this.writeAuthorityResolver = new GeneratedWriteAuthorityResolver(
                catalogRegistry, dataSource, this, modelAuthority);
    }

    /** Captures current INVOKER permissions and the exact connector declaration before allocation. */
    public WriteAuthorizationReceipt writeAuthority(
            org.pangrova.coordinator.query.execution.GeneratedWriteAuthoritySupplier.WriteAuthorityRequest request) {
        return writeAuthorityResolver.resolve(request);
    }

    /** Keeps plain-table write authority active on query-only coordinators. */
    public static org.pangrova.coordinator.query.execution.GeneratedWriteAuthoritySupplier writeAuthoritySupplier(
            CatalogRegistry catalogs, DataSource dataSource, GeneratedValueMaterializationCoordinator owner,
            CurrentModelAuthority modelAuthority) {
        return new GeneratedWriteAuthorityResolver(catalogs, dataSource, owner, modelAuthority)::resolve;
    }

    public void bindSourceCompiler(GeneratedSourceCompiler compiler) {
        buildService.bindSourceCompiler(compiler);
    }

    GeneratedSourceReads.ObservedRead compileSource(QualifiedTableName table, java.util.List<String> columns,
                                                   ConnectorSession session) {
        return buildService.compileSource(table, columns, session);
    }

    /** Validates an ASYNC write before any connector mutation begins. */
    public void activateCatalogs(Set<String> catalogNames) {
        Objects.requireNonNull(catalogNames, "catalogNames must not be null");
        if (catalogNames.stream().anyMatch(name -> name == null || name.isBlank())) {
            throw new IllegalArgumentException("catalog names must not be blank");
        }
        registeredCatalogs = Set.copyOf(catalogNames);
    }

    /** Rejects definitions without a durable listener or an exact supported Native model/materializer. */
    public void requireWriteAdmission(
            QualifiedTableName table, GeneratedColumnSet definitions,
            UserIdentity user, String operation) {
        Objects.requireNonNull(table, "table must not be null");
        GeneratedColumnMetadataCodec.validate(definitions);
        if (!registeredCatalogs.contains(table.catalog())) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    operation + " targets generated columns in catalog '"
                            + table.catalog() + "', but no durable connector commit "
                            + "listener is registered for that catalog");
        }
        if (definitions.getDefinitionsList().stream().anyMatch(definition ->
                definition.getExecutionMode() == ExecutionMode.EXECUTION_MODE_SYNC)) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    operation + " targets a SYNC generated column, but the Native write "
                            + "relation does not contain its generated expression in the same "
                            + "atomic commit");
        }
        if (definitions.getDefinitionsList().stream().anyMatch(definition ->
                definition.getExecutionMode() != ExecutionMode.EXECUTION_MODE_ASYNC)) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    operation + " has an unknown generated-column execution mode");
        }
        buildService.requireExecutable(definitions, user);
    }

    /** Compiles one exact connector commit into idempotent Native work pages. */
    @Override
    public void onCommit(CommitEvent event) {
        if (!registeredCatalogs.contains(event.catalogName())) {
            throw new IllegalStateException(
                    "Generated-value commit received from an unregistered catalog: "
                            + event.catalogName());
        }
        QualifiedTableName table = new QualifiedTableName(
                event.catalogName(), event.schemaName(), event.tableName());
        if (event.commitFingerprint() == null) {
            throw new AuthorizationDeniedException("Deferred maintenance requires its authenticated redo anchor");
        }
        WriteCommitEntry entry = writeJournal.findByFingerprint(event.commitFingerprint()).orElseThrow(() ->
                new AuthorizationDeniedException("Deferred commit lost its authenticated write journal anchor"));
        requireMatchingCommit(event, entry, table);
        if (writeJournal.isDeferredWorkAcknowledged(entry.commitFingerprint())) {
            return;
        }
        if (!entry.authorizationReceipt().declaration().hasGeneratedColumns()) {
            writeJournal.acknowledgeNoDeferredWork(entry.commitFingerprint());
            return;
        }
        ConnectorSession session = new MaterializationSession(table, entry.principal(), entry.tenantId());
        CatalogHandle catalog = catalogRegistry.getCatalog(event.catalogName());
        ConnectorMetadata metadata = catalog.getMetadata(session);
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(event.schemaName(), event.tableName()));
        if (handle == null) {
            throw new IllegalStateException(
                    "Committed table disappeared before generated-value scheduling: " + table);
        }
        var currentMetadata = metadata.getTableMetadata(session, handle);
        GeneratedColumnSet definitions = GeneratedColumnMetadataCodec.decode(currentMetadata.properties());
        var currentDeclaration = WriteDeclarationSnapshot.capture(table, currentMetadata, definitions.toByteArray(),
                definitions.getDefinitionsCount() != 0);
        if (!currentDeclaration.equals(entry.authorizationReceipt().declaration())) {
            throw new AuthorizationDeniedException("Connector declaration changed; securely resubmit maintenance");
        }
        if (definitions.getDefinitionsList().stream().anyMatch(definition ->
                definition.getExecutionMode() != ExecutionMode.EXECUTION_MODE_ASYNC)) {
            throw new IllegalStateException("Committed generated intent contains unsupported execution mode");
        }
        buildService.submitCommitted(entry, definitions, table, event.snapshotId(), session);
    }

    private static void requireMatchingCommit(CommitEvent event, WriteCommitEntry entry, QualifiedTableName table) {
        if (entry.state() != WriteCommitEntry.State.COMMITTED || !entry.tenantId().equals(event.tenantId())
                || !entry.catalogName().equals(event.catalogName())
                || !entry.authorizationReceipt().declaration().table().equals(table)) {
            throw new AuthorizationDeniedException("Commit event differs from its authenticated write journal anchor");
        }
    }

    private record MaterializationSession(QualifiedTableName table, String principal, String tenant)
            implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return principal; }
        @Override public java.util.Optional<String> getCatalog() {
            return java.util.Optional.of(table.catalog());
        }
        @Override public java.util.Optional<String> getSchema() {
            return java.util.Optional.of(table.schema());
        }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", tenant); }
    }
}
