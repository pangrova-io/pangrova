package org.pangrova.coordinator.query.execution.dispatch.task.file;

import org.pangrova.metadata.core.resolution.QualifiedTableNames;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.parser.api.ast.statement.AlterStageAction;
import org.pangrova.parser.api.ast.statement.AlterStageStatement;
import org.pangrova.parser.api.ast.statement.CreateStageStatement;
import org.pangrova.parser.api.ast.statement.DescribeStageStatement;
import org.pangrova.parser.api.ast.statement.DropStageStatement;
import org.pangrova.parser.api.ast.statement.ShowStagesStatement;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.spi.authorization.AccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.space.FileSpaceMutation;
import org.pangrova.persistence.file.api.space.FileSpaceRegistry;
import org.pangrova.file.api.space.FileSpaceType;

/** Registers the Stage metadata tasks; all operations share one registry row model. */
public final class StageTasks {

    private static final List<String> COLUMNS = List.of(
            "name", "type", "url", "storage_profile", "directory_enabled", "comment", "owner");

    private static final List<String> DESCRIBE_COLUMNS = List.of(
            "name", "type", "url", "storage_profile", "directory_enabled", "comment", "owner",
            "created_at", "updated_at");

    private StageTasks() {}

    /** Registers the Stage metadata tasks; all operations share one registry row model. */
    public static void register(MetadataExecutionFactory factory, FileSpaceRegistry registry,
            AccessControl accessControl, FileSpaceAuditEmitter audit) {
        register(factory, registry, accessControl, audit, null);
    }

    /** Installs directory refresh when the file plane and durable index are configured. */
    public static void register(MetadataExecutionFactory factory, FileSpaceRegistry registry,
            AccessControl accessControl, FileSpaceAuditEmitter audit,
            org.pangrova.file.core.directory.StageDirectoryRefreshService refresh) {
        factory.registerTask(CreateStageStatement.class,
                (statement, context) -> create(registry, audit, statement, context));
        factory.registerTask(AlterStageStatement.class,
                (statement, context) -> alter(registry, audit, refresh, statement, context));
        factory.registerTask(DropStageStatement.class,
                (statement, context) -> drop(registry, audit, statement, context));
        factory.registerTask(ShowStagesStatement.class,
                (statement, context) -> show(registry, accessControl, statement, context));
        factory.registerTask(DescribeStageStatement.class,
                (statement, context) -> describe(registry, accessControl, statement, context));
    }

    private static StatementResult create(FileSpaceRegistry registry, FileSpaceAuditEmitter audit,
            CreateStageStatement statement, StatementExecutionContext context) {
        QuerySession session = context.session();
        QualifiedTableName ref = FileSpaceTaskSupport.resolve(statement.name(), session);
        FileSpaceDefinition definition = new FileSpaceDefinition(FileSpaceType.STAGE,
                ref.catalog(), ref.schema(), ref.table(), statement.external(),
                statement.storageProfile().orElse(""), statement.url().orElse(""),
                statement.directoryEnabled(), statement.comment().orElse(""),
                FileSpaceTaskSupport.requireUser(session).principal());
        boolean existed = registry.find(FileSpaceType.STAGE, ref.catalog(), ref.schema(),
                ref.table()).isPresent();
        FileSpace space = registry.create(definition, statement.ifNotExists());
        if (!existed) {
            audit.created(context.auditAttribution(), space);
        }
        String message = existed && statement.ifNotExists()
                ? "Stage already exists: " + space.qualifiedName()
                : "Stage created: " + space.qualifiedName();
        return new StatementResult(List.of(), List.of(), message, StatementCategory.DDL);
    }

    private static StatementResult alter(FileSpaceRegistry registry, FileSpaceAuditEmitter audit,
            org.pangrova.file.core.directory.StageDirectoryRefreshService refresh,
            AlterStageStatement statement, StatementExecutionContext context) {
        QuerySession session = context.session();
        QualifiedTableName ref = FileSpaceTaskSupport.resolve(statement.name(), session);
        FileSpace current = FileSpaceTaskSupport.require(registry, FileSpaceType.STAGE, ref);
        AlterStageAction action = statement.action();
        if (action instanceof AlterStageAction.Rename rename) {
            return rename(registry, audit, context, ref, current, rename);
        }
        if (action instanceof AlterStageAction.Refresh requested) {
            if (refresh == null) {
                throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED,
                        "ALTER STAGE ... REFRESH requires the file plane and directory index");
            }
            long files = refresh.refresh(FileSpaceTaskSupport.requireUser(session), current,
                    requested.subpath().orElse(""));
            audit.altered(context.auditAttribution(), current, "REFRESH", Map.of("files", Long.toString(files),
                    "subpath", requested.subpath().orElse("")));
            return new StatementResult(List.of(), List.of(),
                    "Stage directory refreshed: " + files + " files", StatementCategory.DDL);
        }
        FileSpaceMutation mutation;
        String actionName;
        Map<String, String> extra = Map.of();
        if (action instanceof AlterStageAction.SetUrl url) {
            mutation = new FileSpaceMutation.SetLocation(url.url());
            actionName = "SET_URL";
        } else if (action instanceof AlterStageAction.SetStorageProfile profile) {
            mutation = new FileSpaceMutation.SetStorageProfile(profile.storageProfile());
            actionName = "SET_STORAGE_PROFILE";
        } else if (action instanceof AlterStageAction.SetDirectoryEnabled directory) {
            mutation = new FileSpaceMutation.SetDirectoryEnabled(directory.enabled());
            actionName = "SET_DIRECTORY";
        } else if (action instanceof AlterStageAction.SetComment comment) {
            mutation = new FileSpaceMutation.SetComment(comment.comment());
            actionName = "SET_COMMENT";
        } else {
            AlterStageAction.SetOwner owner = (AlterStageAction.SetOwner) action;
            mutation = new FileSpaceMutation.SetOwner(owner.owner());
            actionName = "SET_OWNER";
            extra = Map.of("previous_owner", current.owner());
        }
        FileSpace updated = registry.mutate(FileSpaceType.STAGE, ref.catalog(), ref.schema(),
                ref.table(), mutation);
        audit.altered(context.auditAttribution(), updated, actionName, extra);
        return new StatementResult(List.of(), List.of(),
                "Stage altered: " + updated.qualifiedName(), StatementCategory.DDL);
    }

    private static StatementResult rename(FileSpaceRegistry registry, FileSpaceAuditEmitter audit,
            StatementExecutionContext context, QualifiedTableName ref, FileSpace current,
            AlterStageAction.Rename rename) {
        QualifiedTableName target = QualifiedTableNames.resolveRenameTarget(rename.newName().parts(), ref);
        if (!target.schema().equals(ref.schema())) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "ALTER STAGE RENAME cannot move a stage between schemas");
        }
        FileSpace renamed = registry.rename(FileSpaceType.STAGE, ref.catalog(), ref.schema(),
                ref.table(), target.table());
        audit.altered(context.auditAttribution(), renamed, "RENAME", Map.of("previous_name", current.name()));
        return new StatementResult(List.of(), List.of(),
                "Stage renamed: " + renamed.qualifiedName(), StatementCategory.DDL);
    }

    private static StatementResult drop(FileSpaceRegistry registry, FileSpaceAuditEmitter audit,
            DropStageStatement statement, StatementExecutionContext context) {
        QualifiedTableName ref = FileSpaceTaskSupport.resolve(statement.name(), context.session());
        Optional<FileSpace> existing = registry.find(FileSpaceType.STAGE, ref.catalog(),
                ref.schema(), ref.table());
        if (existing.isEmpty()) {
            if (!statement.ifExists()) {
                throw FileSpaceTaskSupport.notFound(FileSpaceType.STAGE, ref);
            }
            return new StatementResult(List.of(), List.of(),
                    "Stage does not exist: " + ref, StatementCategory.DDL);
        }
        registry.drop(FileSpaceType.STAGE, ref.catalog(), ref.schema(), ref.table(), false);
        audit.dropped(context.auditAttribution(), existing.get());
        return new StatementResult(List.of(), List.of(),
                "Stage dropped: " + existing.get().qualifiedName(), StatementCategory.DDL);
    }

    private static StatementResult show(FileSpaceRegistry registry, AccessControl accessControl,
            ShowStagesStatement statement, StatementExecutionContext context) {
        QuerySession session = context.session();
        UserIdentity user = FileSpaceTaskSupport.requireUser(session);
        AccessControl ac = FileSpaceTaskSupport.requireAccessControl(accessControl);
        FileSpaceTaskSupport.Scope scope =
                FileSpaceTaskSupport.resolveScope(statement.qualifiedName(), session);
        List<TypedRow> rows = registry.list(FileSpaceType.STAGE, scope.catalog(), scope.schema())
                .stream()
                .filter(space -> FileSpaceTaskSupport.canUseStage(ac, user, space))
                .map(space -> {
                    TypedRow.Builder row = TypedRow.builder();
                    row.putRaw("name", space.name());
                    row.putRaw("type", space.external() ? "EXTERNAL" : "INTERNAL");
                    row.putRaw("url", space.location());
                    row.putRaw("storage_profile", space.storageProfile());
                    row.putRaw("directory_enabled", space.directoryEnabled());
                    row.putRaw("comment", space.comment());
                    row.putRaw("owner", space.owner());
                    return row.build();
                })
                .toList();
        return new StatementResult(rows, COLUMNS, null, StatementCategory.QUERY);
    }

    private static StatementResult describe(FileSpaceRegistry registry, AccessControl accessControl,
            DescribeStageStatement statement, StatementExecutionContext context) {
        QuerySession session = context.session();
        QualifiedTableName ref = FileSpaceTaskSupport.resolve(statement.name(), session);
        FileSpace space = FileSpaceTaskSupport.require(registry, FileSpaceType.STAGE, ref);
        UserIdentity user = FileSpaceTaskSupport.requireUser(session);
        FileSpaceTaskSupport.requireAccessControl(accessControl)
                .checkCanUseStage(user, space.catalog(), space.schema(), space.name());
        TypedRow.Builder row = TypedRow.builder();
        row.putRaw("name", space.name());
        row.putRaw("type", space.external() ? "EXTERNAL" : "INTERNAL");
        row.putRaw("url", space.location());
        row.putRaw("storage_profile", space.storageProfile());
        row.putRaw("directory_enabled", space.directoryEnabled());
        row.putRaw("comment", space.comment());
        row.putRaw("owner", space.owner());
        row.putRaw("created_at", space.createdAt().toString());
        row.putRaw("updated_at", space.updatedAt().toString());
        return new StatementResult(List.of(row.build()), DESCRIBE_COLUMNS, null,
                StatementCategory.QUERY);
    }
}
