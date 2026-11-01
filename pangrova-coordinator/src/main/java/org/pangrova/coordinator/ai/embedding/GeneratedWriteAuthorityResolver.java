package org.pangrova.coordinator.ai.embedding;

import org.pangrova.ai.store.jdbc.GeneratedModelCurrentAuthority;
import org.pangrova.coordinator.query.execution.GeneratedWriteAuthoritySupplier.WriteAuthorityRequest;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.security.tenant.store.JdbcTenantStore;
import org.pangrova.protocol.GeneratedColumnMetadataCodec;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.security.authz.DeferredAuthorizationScopeCodec;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.WriteAuthorizationReceipt;
import org.pangrova.spi.coordination.WriteDeclarationSnapshot;
import org.pangrova.spi.security.UserIdentity;

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

    GeneratedWriteAuthorityResolver(CatalogRegistry catalogs, DataSource dataSource,
                                     GeneratedValueMaterializationCoordinator coordinator) {
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.coordinator = coordinator;
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
            org.pangrova.security.store.AuthorizationTransactionGate.lockPublication(connection);
            var models = modelNames.isEmpty() ? List.<DeferredAuthorizationScope.ModelSnapshot>of()
                    : GeneratedModelCurrentAuthority.observe(connection, principal, modelNames);
            var permissions = new ArrayList<>(reads);
            request.requiredWritePrivileges().forEach(privilege -> permissions.add(
                    new DeferredAuthorizationScope.TablePermission(
                    request.table(), Set.of(), privilege, DeferredAuthorizationScope.ReadMode.NONE,
                    DeferredAuthorizationScope.PolicySnapshot.empty())));
            var scope = new DeferredAuthorizationScope(permissions,
                    Optional.of(new DeferredAuthorizationScope.DefinitionSnapshot(declaration, models)));
            JdbcTenantStore.requireDeferredTenant(connection, principal, tenant, scope,
                    request.internalMaterializationExecution()
                            ? org.pangrova.coordinator.query.security.AccessControlEnforcer
                                    ::isReservedDynamicTableGeneration
                            : table -> false);
            if (!request.internalMaterializationExecution()) {
                org.pangrova.metadata.core.TableControlDeferredPolicy.requireCurrent(connection, principal, scope);
            }
            var receipt = DeferredAuthorizationScopeCodec.encodeWriteReceipt(scope);
            connection.commit();
            return receipt;
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot observe current generated write authority", failure);
        }
    }

}
