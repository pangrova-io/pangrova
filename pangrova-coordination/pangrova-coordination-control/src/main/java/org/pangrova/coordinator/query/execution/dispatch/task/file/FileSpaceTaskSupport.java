package org.pangrova.coordinator.query.execution.dispatch.task.file;

import org.pangrova.query.session.SessionTableNames;
import java.util.List;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.persistence.file.api.space.FileSpaceRegistry;
import org.pangrova.file.api.enums.FileSpaceType;

/** Shared resolution rules for Volume and Stage metadata tasks. */
final class FileSpaceTaskSupport {

    private FileSpaceTaskSupport() {}

    static QualifiedTableName resolve(Identifier name, QuerySession session) {
        return SessionTableNames.resolveTableRef(name.parts(), session);
    }

    /** SHOW accepts no qualifier, a schema, or catalog.schema. */
    static Scope resolveScope(List<String> parts, QuerySession session) {
        return switch (parts.size()) {
            case 0 -> new Scope(SessionTableNames.defaultCatalog(session),
                    SessionTableNames.defaultSchema(session));
            case 1 -> new Scope(SessionTableNames.defaultCatalog(session), parts.get(0));
            case 2 -> new Scope(parts.get(0), parts.get(1));
            default -> throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "SHOW accepts an optional schema or catalog.schema qualifier");
        };
    }

    static FileSpace require(FileSpaceRegistry registry, FileSpaceType type,
            QualifiedTableName ref) {
        return registry.find(type, ref.catalog(), ref.schema(), ref.table())
                .orElseThrow(() -> notFound(type, ref));
    }

    static PangrovaException notFound(FileSpaceType type, QualifiedTableName ref) {
        return new PangrovaException(PangrovaErrorCode.METADATA_FILE_SPACE_NOT_FOUND,
                kind(type) + " not found: " + ref.catalog() + "." + ref.schema() + "."
                        + ref.table());
    }

    static UserIdentity requireUser(QuerySession session) {
        UserIdentity user = session == null ? null : session.userIdentity();
        if (user == null) {
            throw new AccessDeniedException("Authenticated volume or stage principal is required");
        }
        return user;
    }

    static AccessControl requireAccessControl(AccessControl accessControl) {
        if (accessControl == null) {
            throw new AccessDeniedException("Access control is required for volume and stage access");
        }
        return accessControl;
    }

    static boolean canReadVolume(AccessControl accessControl, UserIdentity user, FileSpace space) {
        try {
            accessControl.checkCanReadVolume(user, space.catalog(), space.schema(), space.name());
            return true;
        } catch (AccessDeniedException denied) {
            return false;
        }
    }

    static boolean canUseStage(AccessControl accessControl, UserIdentity user, FileSpace space) {
        try {
            accessControl.checkCanUseStage(user, space.catalog(), space.schema(), space.name());
            return true;
        } catch (AccessDeniedException denied) {
            return false;
        }
    }

    static String kind(FileSpaceType type) {
        return type == FileSpaceType.VOLUME ? "Volume" : "Stage";
    }

    record Scope(String catalog, String schema) {}
}
