package org.pangrova.coordinator.ai.branch;

import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.query.core.session.SessionTableNames;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.parser.api.ast.statement.CreateBranchStatement;
import org.pangrova.parser.api.ast.statement.CreateTagStatement;
import org.pangrova.parser.api.ast.statement.DropBranchStatement;
import org.pangrova.parser.api.ast.statement.DropTagStatement;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.security.spi.authorization.AccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Executes branch and tag DDL statements by dispatching to connector-specific
 * implementations (Iceberg ManageSnapshots / Paimon native branch API).
 *
 * <p>Handles: CREATE/DROP BRANCH and CREATE/DROP TAG.
 * Enforces ALTER privilege via RBAC before mutation operations.</p>
 */
public final class BranchDdlExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(BranchDdlExecutor.class);

    private final BranchDdlContext deps;

    public BranchDdlExecutor(BranchDdlContext deps) {
        this.deps = deps;
    }

    /** Executes CREATE BRANCH after enforcing ALTER privileges. */
    public List<TypedRow> executeCreateBranch(CreateBranchStatement stmt, QuerySession context) {
        QualifiedTableName tableRef = SessionTableNames.resolveTableRef(stmt.tableName().parts(), context);
        checkAlterPrivilege(tableRef, context.userIdentity());

        ConnectorSession session = SessionContextConverter.toConnectorSession(context, tableRef.catalog(),
                tableRef.schema());
        ConnectorMetadata metadata = resolveMetadata(tableRef, session);
        ConnectorTableHandle tableHandle = requireTableHandle(metadata, session, tableRef);

        try {
            metadata.createBranch(session, tableHandle, stmt.branchName(),
                    new org.pangrova.connector.api.metadata.BranchCreationOptions(
                            stmt.fromSnapshot(), stmt.fromTag(), java.util.OptionalLong.empty()));
        } catch (UnsupportedOperationException e) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_DDL_FAILED,
                    "Connector for catalog '%s' does not support branch operations"
                            .formatted(tableRef.catalog()));
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                throw new PangrovaException(PangrovaErrorCode.AI_BRANCH_ALREADY_EXISTS,
                        "Branch '%s' already exists on table %s"
                                .formatted(stmt.branchName(), tableRef));
            }
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_DDL_FAILED, e.getMessage());
        }

        LOG.info("action=create_branch table={} branch={}", tableRef, stmt.branchName());
        return List.of(TypedRow.builder()
                .putString("result", "BRANCH CREATED")
                .putString("table", tableRef.toString())
                .putString("branch", stmt.branchName())
                .build());
    }

    /** Executes DROP BRANCH after enforcing ALTER privileges. */
    public List<TypedRow> executeDropBranch(DropBranchStatement stmt, QuerySession context) {
        QualifiedTableName tableRef = SessionTableNames.resolveTableRef(stmt.tableName().parts(), context);
        checkAlterPrivilege(tableRef, context.userIdentity());

        ConnectorSession session = SessionContextConverter.toConnectorSession(context, tableRef.catalog(),
                tableRef.schema());
        ConnectorMetadata metadata = resolveMetadata(tableRef, session);
        ConnectorTableHandle tableHandle = requireTableHandle(metadata, session, tableRef);

        try {
            metadata.dropBranch(session, tableHandle, stmt.branchName());
        } catch (UnsupportedOperationException e) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_DDL_FAILED,
                    "Connector for catalog '%s' does not support branch operations"
                            .formatted(tableRef.catalog()));
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && e.getMessage().contains("not found")) {
                throw new PangrovaException(PangrovaErrorCode.AI_BRANCH_NOT_FOUND,
                        "Branch '%s' not found on table %s"
                                .formatted(stmt.branchName(), tableRef));
            }
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_DDL_FAILED, e.getMessage());
        }

        LOG.info("action=drop_branch table={} branch={}", tableRef, stmt.branchName());
        return List.of(TypedRow.builder()
                .putString("result", "BRANCH DROPPED")
                .putString("table", tableRef.toString())
                .putString("branch", stmt.branchName())
                .build());
    }

    /** Executes CREATE TAG after enforcing ALTER privileges. */
    public List<TypedRow> executeCreateTag(CreateTagStatement stmt, QuerySession context) {
        QualifiedTableName tableRef = SessionTableNames.resolveTableRef(stmt.tableName().parts(), context);
        checkAlterPrivilege(tableRef, context.userIdentity());

        ConnectorSession session = SessionContextConverter.toConnectorSession(context, tableRef.catalog(),
                tableRef.schema());
        ConnectorMetadata metadata = resolveMetadata(tableRef, session);
        ConnectorTableHandle tableHandle = requireTableHandle(metadata, session, tableRef);

        try {
            metadata.createTag(session, tableHandle, stmt.tagName());
        } catch (UnsupportedOperationException e) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_DDL_FAILED,
                    "Connector for catalog '%s' does not support tag operations"
                            .formatted(tableRef.catalog()));
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                throw new PangrovaException(PangrovaErrorCode.AI_BRANCH_ALREADY_EXISTS,
                        "Tag '%s' already exists on table %s"
                                .formatted(stmt.tagName(), tableRef));
            }
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_DDL_FAILED, e.getMessage());
        }

        LOG.info("action=create_tag table={} tag={}", tableRef, stmt.tagName());
        return List.of(TypedRow.builder()
                .putString("result", "TAG CREATED")
                .putString("table", tableRef.toString())
                .putString("tag", stmt.tagName())
                .build());
    }

    /** Executes DROP TAG after enforcing ALTER privileges. */
    public List<TypedRow> executeDropTag(DropTagStatement stmt, QuerySession context) {
        QualifiedTableName tableRef = SessionTableNames.resolveTableRef(stmt.tableName().parts(), context);
        checkAlterPrivilege(tableRef, context.userIdentity());

        ConnectorSession session = SessionContextConverter.toConnectorSession(context, tableRef.catalog(),
                tableRef.schema());
        ConnectorMetadata metadata = resolveMetadata(tableRef, session);
        ConnectorTableHandle tableHandle = requireTableHandle(metadata, session, tableRef);

        try {
            metadata.dropTag(session, tableHandle, stmt.tagName());
        } catch (UnsupportedOperationException e) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_DDL_FAILED,
                    "Connector for catalog '%s' does not support tag operations"
                            .formatted(tableRef.catalog()));
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && e.getMessage().contains("not found")) {
                throw new PangrovaException(PangrovaErrorCode.AI_BRANCH_NOT_FOUND,
                        "Tag '%s' not found on table %s"
                                .formatted(stmt.tagName(), tableRef));
            }
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_DDL_FAILED, e.getMessage());
        }

        LOG.info("action=drop_tag table={} tag={}", tableRef, stmt.tagName());
        return List.of(TypedRow.builder()
                .putString("result", "TAG DROPPED")
                .putString("table", tableRef.toString())
                .putString("tag", stmt.tagName())
                .build());
    }

    private void checkAlterPrivilege(QualifiedTableName tableRef, UserIdentity user) {
        AccessControl ac = deps.accessControl();
        if (ac == null || user == null) {
            return;
        }
        ac.checkCanAlterTable(user, tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    private ConnectorMetadata resolveMetadata(QualifiedTableName tableRef, ConnectorSession session) {
        CatalogHandle handle = deps.catalogRegistry().getCatalog(tableRef.catalog());
        return handle.getMetadata(session);
    }

    private static ConnectorTableHandle requireTableHandle(
            ConnectorMetadata metadata, ConnectorSession session, QualifiedTableName ref) {
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(ref.schema(), ref.table()));
        if (handle == null) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + ref);
        }
        return handle;
    }
}
