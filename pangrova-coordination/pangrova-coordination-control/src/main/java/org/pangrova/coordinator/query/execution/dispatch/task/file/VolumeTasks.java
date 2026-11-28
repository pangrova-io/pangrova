package org.pangrova.coordinator.query.execution.dispatch.task.file;

import org.pangrova.metadata.resolution.QualifiedTableNames;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.parser.api.ast.statement.AlterVolumeAction;
import org.pangrova.parser.api.ast.statement.AlterVolumeStatement;
import org.pangrova.parser.api.ast.statement.CreateVolumeStatement;
import org.pangrova.parser.api.ast.statement.DescribeVolumeStatement;
import org.pangrova.parser.api.ast.statement.DropVolumeStatement;
import org.pangrova.parser.api.ast.statement.ShowVolumesStatement;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.space.FileSpaceMutation;
import org.pangrova.persistence.file.api.space.FileSpaceRegistry;
import org.pangrova.file.api.enums.FileSpaceType;

/** Registers the Volume metadata tasks; all operations share one registry row model. */
public final class VolumeTasks {

    private static final List<String> COLUMNS = List.of(
            "name", "type", "storage_profile", "location", "comment", "owner");

    private static final List<String> DESCRIBE_COLUMNS = List.of(
            "name", "type", "storage_profile", "location", "comment", "owner",
            "created_at", "updated_at");

    private VolumeTasks() {}

    /** Registers the Volume metadata tasks; all operations share one registry row model. */
    public static void register(MetadataExecutionFactory factory, FileSpaceRegistry registry,
            AccessControl accessControl, FileSpaceAuditEmitter audit) {
        factory.registerTask(CreateVolumeStatement.class,
                (statement, context) -> create(registry, audit, statement, context));
        factory.registerTask(AlterVolumeStatement.class,
                (statement, context) -> alter(registry, audit, statement, context));
        factory.registerTask(DropVolumeStatement.class,
                (statement, context) -> drop(registry, audit, statement, context));
        factory.registerTask(ShowVolumesStatement.class,
                (statement, context) -> show(registry, accessControl, statement, context));
        factory.registerTask(DescribeVolumeStatement.class,
                (statement, context) -> describe(registry, accessControl, statement, context));
    }

    private static StatementResult create(FileSpaceRegistry registry, FileSpaceAuditEmitter audit,
            CreateVolumeStatement statement, StatementExecutionContext context) {
        QuerySession session = context.session();
        QualifiedTableName ref = FileSpaceTaskAuthority.resolve(statement.name(), session);
        FileSpaceDefinition definition = new FileSpaceDefinition(FileSpaceType.VOLUME,
                ref.catalog(), ref.schema(), ref.table(), statement.external(),
                statement.storageProfile().orElse(""), statement.location().orElse(""),
                false, statement.comment().orElse(""),
                FileSpaceTaskAuthority.requireUser(session).principal());
        boolean existed = registry.find(FileSpaceType.VOLUME, ref.catalog(), ref.schema(),
                ref.table()).isPresent();
        FileSpace space = registry.create(definition, statement.ifNotExists());
        if (!existed) {
            audit.created(context.auditAttribution(), space);
        }
        String message = existed && statement.ifNotExists()
                ? "Volume already exists: " + space.qualifiedName()
                : "Volume created: " + space.qualifiedName();
        return new StatementResult(List.of(), List.of(), message, StatementCategory.DDL);
    }

    private static StatementResult alter(FileSpaceRegistry registry, FileSpaceAuditEmitter audit,
            AlterVolumeStatement statement, StatementExecutionContext context) {
        QuerySession session = context.session();
        QualifiedTableName ref = FileSpaceTaskAuthority.resolve(statement.name(), session);
        FileSpace current = FileSpaceTaskAuthority.require(registry, FileSpaceType.VOLUME, ref);
        AlterVolumeAction action = statement.action();
        if (action instanceof AlterVolumeAction.Rename rename) {
            return rename(registry, audit, statement, context, ref, current, rename);
        }
        FileSpaceMutation mutation;
        String actionName;
        Map<String, String> extra = Map.of();
        if (action instanceof AlterVolumeAction.SetStorageProfile profile) {
            mutation = new FileSpaceMutation.SetStorageProfile(profile.storageProfile());
            actionName = "SET_STORAGE_PROFILE";
        } else if (action instanceof AlterVolumeAction.SetComment comment) {
            mutation = new FileSpaceMutation.SetComment(comment.comment());
            actionName = "SET_COMMENT";
        } else {
            AlterVolumeAction.SetOwner owner = (AlterVolumeAction.SetOwner) action;
            mutation = new FileSpaceMutation.SetOwner(owner.owner());
            actionName = "SET_OWNER";
            extra = Map.of("previous_owner", current.owner());
        }
        FileSpace updated = registry.mutate(FileSpaceType.VOLUME, ref.catalog(), ref.schema(),
                ref.table(), mutation);
        audit.altered(context.auditAttribution(), updated, actionName, extra);
        return new StatementResult(List.of(), List.of(),
                "Volume altered: " + updated.qualifiedName(), StatementCategory.DDL);
    }

    private static StatementResult rename(FileSpaceRegistry registry, FileSpaceAuditEmitter audit,
            AlterVolumeStatement statement, StatementExecutionContext context,
            QualifiedTableName ref, FileSpace current, AlterVolumeAction.Rename rename) {
        QualifiedTableName target = QualifiedTableNames.resolveRenameTarget(rename.newName().parts(), ref);
        if (!target.schema().equals(ref.schema())) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "ALTER VOLUME RENAME cannot move a volume between schemas");
        }
        FileSpace renamed = registry.rename(FileSpaceType.VOLUME, ref.catalog(), ref.schema(),
                ref.table(), target.table());
        audit.altered(context.auditAttribution(), renamed, "RENAME", Map.of("previous_name", current.name()));
        return new StatementResult(List.of(), List.of(),
                "Volume renamed: " + renamed.qualifiedName(), StatementCategory.DDL);
    }

    private static StatementResult drop(FileSpaceRegistry registry, FileSpaceAuditEmitter audit,
            DropVolumeStatement statement, StatementExecutionContext context) {
        QualifiedTableName ref = FileSpaceTaskAuthority.resolve(statement.name(), context.session());
        Optional<FileSpace> existing = registry.find(FileSpaceType.VOLUME, ref.catalog(),
                ref.schema(), ref.table());
        if (existing.isEmpty()) {
            if (!statement.ifExists()) {
                throw FileSpaceTaskAuthority.notFound(FileSpaceType.VOLUME, ref);
            }
            return new StatementResult(List.of(), List.of(),
                    "Volume does not exist: " + ref, StatementCategory.DDL);
        }
        registry.drop(FileSpaceType.VOLUME, ref.catalog(), ref.schema(), ref.table(), false);
        audit.dropped(context.auditAttribution(), existing.get());
        return new StatementResult(List.of(), List.of(),
                "Volume dropped: " + existing.get().qualifiedName(), StatementCategory.DDL);
    }

    private static StatementResult show(FileSpaceRegistry registry, AccessControl accessControl,
            ShowVolumesStatement statement, StatementExecutionContext context) {
        QuerySession session = context.session();
        UserIdentity user = FileSpaceTaskAuthority.requireUser(session);
        AccessControl ac = FileSpaceTaskAuthority.requireAccessControl(accessControl);
        FileSpaceTaskAuthority.Scope scope =
                FileSpaceTaskAuthority.resolveScope(statement.qualifiedName(), session);
        List<TypedRow> rows = registry.list(FileSpaceType.VOLUME, scope.catalog(), scope.schema())
                .stream()
                .filter(space -> FileSpaceTaskAuthority.canReadVolume(ac, user, space))
                .map(space -> {
                    TypedRow.Builder row = TypedRow.builder();
                    row.putRaw("name", space.name());
                    row.putRaw("type", space.external() ? "EXTERNAL" : "MANAGED");
                    row.putRaw("storage_profile", space.storageProfile());
                    row.putRaw("location", space.location());
                    row.putRaw("comment", space.comment());
                    row.putRaw("owner", space.owner());
                    return row.build();
                })
                .toList();
        return new StatementResult(rows, COLUMNS, null, StatementCategory.QUERY);
    }

    private static StatementResult describe(FileSpaceRegistry registry, AccessControl accessControl,
            DescribeVolumeStatement statement, StatementExecutionContext context) {
        QuerySession session = context.session();
        QualifiedTableName ref = FileSpaceTaskAuthority.resolve(statement.name(), session);
        FileSpace space = FileSpaceTaskAuthority.require(registry, FileSpaceType.VOLUME, ref);
        UserIdentity user = FileSpaceTaskAuthority.requireUser(session);
        FileSpaceTaskAuthority.requireAccessControl(accessControl)
                .checkCanReadVolume(user, space.catalog(), space.schema(), space.name());
        TypedRow.Builder row = TypedRow.builder();
        row.putRaw("name", space.name());
        row.putRaw("type", space.external() ? "EXTERNAL" : "MANAGED");
        row.putRaw("storage_profile", space.storageProfile());
        row.putRaw("location", space.location());
        row.putRaw("comment", space.comment());
        row.putRaw("owner", space.owner());
        row.putRaw("created_at", space.createdAt().toString());
        row.putRaw("updated_at", space.updatedAt().toString());
        return new StatementResult(List.of(row.build()), DESCRIBE_COLUMNS, null,
                StatementCategory.QUERY);
    }
}
