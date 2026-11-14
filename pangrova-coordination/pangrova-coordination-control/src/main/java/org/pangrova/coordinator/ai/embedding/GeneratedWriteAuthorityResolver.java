package org.pangrova.coordinator.ai.embedding;

import org.pangrova.security.api.enums.AuthorizedReadMode;
import org.pangrova.persistence.workflow.api.security.TableControlWriteAuthority;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;
import org.pangrova.query.api.write.WriteAuthorityRequest;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.jdbc.api.security.policy.AuthorizationPublicationLock;
import org.pangrova.persistence.security.api.tenant.DeferredTenantAuthority;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.security.api.identity.UserIdentity;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Captures the original INVOKER write authority and actual SDK declaration before allocation. */
final class GeneratedWriteAuthorityResolver {
    private final CatalogRegistry catalogs;
    private final DataSource dataSource;
    private final GeneratedValueMaterializationCoordinator coordinator;
    private final CurrentModelAuthority modelAuthority;
    private final DeferredTenantAuthority tenantAuthority;
    private final AuthorizationPublicationLock publicationLock;
    private final TableControlWriteAuthority tableAuthority;

    GeneratedWriteAuthorityResolver(CatalogRegistry catalogs, DataSource dataSource,
                                     GeneratedValueMaterializationCoordinator coordinator,
                                     CurrentModelAuthority modelAuthority,
                                     DeferredTenantAuthority tenantAuthority,
                                     AuthorizationPublicationLock publicationLock,
                                     TableControlWriteAuthority tableAuthority) {
        this.tableAuthority = Objects.requireNonNull(tableAuthority, "tableAuthority");
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.coordinator = coordinator;
        this.modelAuthority = Objects.requireNonNull(modelAuthority, "modelAuthority");
        this.tenantAuthority = Objects.requireNonNull(tenantAuthority, "tenantAuthority");
        this.publicationLock = Objects.requireNonNull(publicationLock, "publicationLock");
    }

    WriteAuthorizationReceipt resolve(WriteAuthorityRequest request) {
        QualifiedTableName table = request.table();
        ConnectorSession session = request.session();
        String tenant = session.getProperties().get("tenant_id");
        if (tenant == null || tenant.isBlank()) {
            throw new AuthorizationDeniedException("Write receipt requires the resolved authenticated tenant");
        }
        var metadata = catalogs.getCatalog(table.catalog()).getMetadata(session);
        var handle = metadata.getTableHandle(session, new SchemaTableName(table.schema(), table.table()));
        if (handle == null) {
            throw new AuthorizationDeniedException("Write declaration table is absent: " + table);
        }
        var actual = metadata.getTableMetadata(session, handle);
        var definitions = GeneratedColumnMetadataCodec.decode(actual.properties());
        var declaration = WriteDeclarationSnapshot.capture(table, actual, definitions.toByteArray(),
                definitions.getDefinitionsCount() != 0);
        if (!declaration.equals(request.compiledDeclaration())) {
            throw new AuthorizationDeniedException("Compiled write declaration changed during authority resolution");
        }
        var user = new UserIdentity(session.getUser(), Set.of(), Map.of("tenant_id", tenant), Instant.EPOCH);
        if (declaration.hasGeneratedColumns()) {
            if (coordinator == null) {
                throw new AuthorizationDeniedException(
                        "Generated-column writes require an active durable materialization owner");
            }
            coordinator.requireWriteAdmission(table, definitions, user, request.writeMode());
        }
        Set<String> sourceColumns = definitions.getDefinitionsList().stream()
                .flatMap(definition -> definition.getSourceColumnsList().stream()).collect(Collectors.toSet());
        Set<String> models = definitions.getDefinitionsList().stream()
                .map(definition -> definition.getLogicalModelName()).collect(Collectors.toSet());
        var reads = new ArrayList<>(request.observedReadPermissions());
        if (!sourceColumns.isEmpty()) {
            reads.add(coordinator.compileSource(table, sourceColumns.stream().sorted().toList(), session).permission());
        }
        return currentReceipt(request, tenant, declaration, GeneratedWriteReadPermissions.merge(reads), models);
    }

    private WriteAuthorizationReceipt currentReceipt(WriteAuthorityRequest request, String tenant,
            WriteDeclarationSnapshot declaration, List<DeferredAuthorizationScope.TablePermission> reads,
            Set<String> modelNames) {
        String principal = request.session().getUser();
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                return authorizeAndEncode(connection, request, principal, tenant, declaration, reads, modelNames);
            } catch (SQLException | RuntimeException failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollback) {
                    failure.addSuppressed(rollback);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot observe current generated write authority", failure);
        }
    }

    private WriteAuthorizationReceipt authorizeAndEncode(java.sql.Connection connection,
        WriteAuthorityRequest request, String principal, String tenant,
        WriteDeclarationSnapshot declaration, List<DeferredAuthorizationScope.TablePermission> reads,
        Set<String> modelNames) throws SQLException {
        publicationLock.lockPublication(connection);
        var models = modelNames.isEmpty() ? List.<DeferredAuthorizationScope.ModelSnapshot>of()
                : modelAuthority.observe(connection, principal, modelNames);
        var permissions = new ArrayList<>(reads);
        request.requiredWritePrivileges().forEach(privilege -> permissions.add(
                new DeferredAuthorizationScope.TablePermission(
                request.table(), Set.of(), privilege, AuthorizedReadMode.NONE,
                DeferredAuthorizationScope.PolicySnapshot.empty())));
        var scope = new DeferredAuthorizationScope(permissions,
                Optional.of(new DeferredAuthorizationScope.DefinitionSnapshot(declaration, models)));
        tenantAuthority.requireCurrent(connection, principal, tenant, scope,
                request.internalMaterializationExecution()
                        ? org.pangrova.coordinator.query.security.AccessControlEnforcer
                                ::isReservedDynamicTableGeneration
                        : table -> false);
        if (!request.internalMaterializationExecution()) {
            tableAuthority.requireCurrent(connection,
                    principal, scope);
        }
        var receipt = DeferredAuthorizationScopeCodec.encodeWriteReceipt(scope);
        connection.commit();
        return receipt;
    }

}
