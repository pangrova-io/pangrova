package org.pangrova.materialization.api.port;

import org.pangrova.materialization.api.enums.MaterializationAction;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Set;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.domain.Principal;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.domain.MaterializationWriteCapability;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.connector.api.materialization.SourceRef;

/**
 * Authorization boundary of the materialization kernel.
 *
 * <p>Creation requires source read rights for the definer and target schema
 * create rights for the requester. Managing an object requires ownership or the
 * corresponding object privilege. Reading materialized data requires only the
 * object privilege; definer semantics mean source privileges are not rechecked
 * per query.
 */
@Experimental
public interface MaterializationAuthorizer {

    /**
     * Requires permission to create an object and to read every source as the definer.
     *
     * @param object object being created
     * @param kind object kind
     * @param requester principal issuing the command
     * @param sources definition sources
     */
    void requireCreate(MaterializationId object, MaterializationKind kind, Principal requester,
            Set<SourceRef> sources);

    /**
     * Requires permission to perform a lifecycle action.
     *
     * @param object target object
     * @param requester principal issuing the command
     * @param action requested action
     */
    void requireManage(MaterializationId object, Principal requester, MaterializationAction action);

    /**
     * Requires permission to read the materialized object.
     *
     * @param object target object
     * @param requester requesting principal
     */
    void requireRead(MaterializationId object, Principal requester);

    /**
     * Issues the internal write capability for a reserved attempt.
     *
     * @param attempt reserved attempt
     * @param generation generation being built
     * @return capability expiring with the attempt fence
     */
    MaterializationWriteCapability issueWriteCapability(RefreshAttempt attempt, GenerationId generation);
}
