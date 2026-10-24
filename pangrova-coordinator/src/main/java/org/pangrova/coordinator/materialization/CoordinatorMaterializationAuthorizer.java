package org.pangrova.coordinator.materialization;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.port.MaterializationAction;
import org.pangrova.materialization.port.MaterializationAuthorizer;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;

/**
 * Access-control adapter for the materialization kernel.
 *
 * <p>Creation checks target schema/table create rights for the requester and
 * source SELECT rights for the definer. Managing an object checks table ALTER
 * or DROP rights; reading checks SELECT on the logical object only, because
 * definer semantics do not re-check source privileges.
 */
public final class CoordinatorMaterializationAuthorizer implements MaterializationAuthorizer {

    private final CoordinatorSessionContext session;
    private final AccessControl accessControl;

    public CoordinatorMaterializationAuthorizer(CoordinatorSessionContext session,
            AccessControl accessControl) {
        this.session = Objects.requireNonNull(session, "session");
        this.accessControl = accessControl;
    }

    @Override
    public void requireCreate(MaterializationId object, MaterializationKind kind,
            Principal requester, Set<SourceRef> sources) {
        requireSessionPrincipal(requester);
        if (accessControl == null) {
            return;
        }
        String catalog = object.name().parts().get(0);
        String schema = object.name().parts().get(1);
        String table = object.name().tableName();
        accessControl.checkCanCreateTable(identity(requester), catalog, schema, table);
        UserIdentity definer = identity(requester);
        for (SourceRef source : sources) {
            accessControl.checkCanSelectFromTable(
                    definer, source.catalog(), source.schema(), source.table());
        }
    }

    @Override
    public void requireManage(MaterializationId object, Principal requester,
            MaterializationAction action) {
        requireSessionPrincipal(requester);
        if (accessControl == null) {
            return;
        }
        String catalog = object.name().parts().get(0);
        String schema = object.name().parts().get(1);
        String table = object.name().tableName();
        UserIdentity identity = identity(requester);
        switch (action) {
            case ALTER, REFRESH -> accessControl.checkCanAlterTable(identity, catalog, schema, table);
            case DROP -> accessControl.checkCanDropTable(identity, catalog, schema, table);
            case SHOW -> accessControl.checkCanShowColumns(identity, catalog, schema, table);
            case SELECT -> accessControl.checkCanSelectFromTable(identity, catalog, schema, table);
            case CREATE -> throw MaterializationException.requestRejected(
                    "CREATE is not a management action for " + object.canonicalName());
            default -> throw new IllegalStateException(
                    "Unhandled materialization action: " + action);
        }
    }

    @Override
    public void requireRead(MaterializationId object, Principal requester) {
        requireSessionPrincipal(requester);
        if (accessControl == null) {
            return;
        }
        accessControl.checkCanSelectFromTable(identity(requester),
                object.name().parts().get(0), object.name().parts().get(1), object.name().tableName());
    }

    @Override
    public MaterializationWriteCapability issueWriteCapability(RefreshAttempt attempt,
            GenerationId generation) {
        Instant expiresAt = attempt.fence().leaseExpiresAt();
        if (!expiresAt.isAfter(attempt.createdAt())) {
            throw MaterializationException.writeCapabilityInvalid(
                    "execution fence for attempt " + attempt.id().value()
                            + " does not cover the reservation instant");
        }
        return new MaterializationWriteCapability(
                attempt.object(),
                attempt.incarnation(),
                attempt.definitionRevision(),
                attempt.id(),
                attempt.candidateTable(),
                generation,
                attempt.sourceVector().canonicalHash(),
                attempt.definer(),
                attempt.fence().controlEpoch(),
                attempt.createdAt(),
                expiresAt);
    }

    private void requireSessionPrincipal(Principal requester) {
        UserIdentity identity = session.userIdentity();
        if (identity == null) {
            throw MaterializationException.requestRejected(
                    "materialization commands require an authenticated principal");
        }
        if (!identity.principal().equals(requester.name())) {
            throw MaterializationException.requestRejected(
                    "materialization command principal " + requester.name()
                            + " does not match the authenticated principal " + identity.principal());
        }
    }

    private UserIdentity identity(Principal principal) {
        UserIdentity current = session.userIdentity();
        if (current != null && current.principal().equals(principal.name())) {
            return current;
        }
        return new UserIdentity(principal.name(), Set.of(), Map.of(), Instant.now());
    }

}
