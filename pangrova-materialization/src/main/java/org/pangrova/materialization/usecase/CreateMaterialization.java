package org.pangrova.materialization.usecase;

import java.util.Map;
import java.util.Optional;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.domain.TargetLag;

/**
 * Command that creates a materialization and publishes its first generation.
 *
 * @param object tenant-scoped logical identity
 * @param kind object kind
 * @param definitionSql definition query
 * @param requester principal issuing the command
 * @param definer principal the definition runs as
 * @param securityMode security mode; only DEFINER is supported
 * @param refreshMode configured refresh mode
 * @param incrementalStrategy configured strategy, empty for AUTO
 * @param targetLag freshness target, empty for manual refresh
 * @param sessionProperties planning and execution options
 * @param commandId idempotency key
 * @param ifNotExists whether an already existing object is left untouched
 */
public record CreateMaterialization(
        MaterializationId object,
        MaterializationKind kind,
        String definitionSql,
        Principal requester,
        Principal definer,
        SecurityMode securityMode,
        RefreshMode refreshMode,
        Optional<StrategyId> incrementalStrategy,
        Optional<TargetLag> targetLag,
        Map<String, String> sessionProperties,
        CommandId commandId,
        boolean ifNotExists) {

    public CreateMaterialization {
        if (object == null || kind == null || requester == null || definer == null
                || securityMode == null || refreshMode == null || commandId == null) {
            throw new IllegalArgumentException(
                    "object, kind, principals, security mode, refresh mode, and command id are required");
        }
        if (definitionSql == null || definitionSql.isBlank()) {
            throw new IllegalArgumentException("definitionSql must not be blank");
        }
        incrementalStrategy = incrementalStrategy == null ? Optional.empty() : incrementalStrategy;
        targetLag = targetLag == null ? Optional.empty() : targetLag;
        sessionProperties = sessionProperties == null ? Map.of() : Map.copyOf(sessionProperties);
        if (object.name().parts().size() != 3) {
            throw new IllegalArgumentException(
                    "materialization names must be fully qualified catalog.schema.table");
        }
    }

    public CreateMaterialization(MaterializationId object, MaterializationKind kind,
            String definitionSql, Principal requester, Principal definer,
            SecurityMode securityMode, RefreshMode refreshMode,
            Optional<StrategyId> incrementalStrategy, Optional<TargetLag> targetLag,
            Map<String, String> sessionProperties, CommandId commandId) {
        this(object, kind, definitionSql, requester, definer, securityMode, refreshMode,
                incrementalStrategy, targetLag, sessionProperties, commandId, false);
    }
}
