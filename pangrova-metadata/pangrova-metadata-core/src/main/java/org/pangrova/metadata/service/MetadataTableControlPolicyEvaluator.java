package org.pangrova.metadata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.pangrova.persistence.jdbc.api.security.policy.AuthorizationPublicationLock;
import org.pangrova.persistence.jdbc.api.security.policy.CurrentDirectRoleReader;
import org.pangrova.persistence.jdbc.api.security.policy.CurrentGrantReader;
import org.pangrova.persistence.jdbc.api.security.policy.CurrentPolicyReader;
import org.pangrova.persistence.workflow.api.security.TableControlPolicyEvaluator;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.PolicySnapshot;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.core.authorization.deferred.DeferredAuthorization;
import org.pangrova.security.core.authorization.deferred.DeferredPolicyReader;

/** Composes Security Core deferred evaluation with the current PostgreSQL authority readers. */
public final class MetadataTableControlPolicyEvaluator implements TableControlPolicyEvaluator {

    private final DeferredAuthorization authorization;
    private final DeferredPolicyReader policies;

    /** Composes the supplied security backend readers on the caller-owned connection. */
    public MetadataTableControlPolicyEvaluator(AuthorizationPublicationLock publicationLock,
            CurrentDirectRoleReader directRoles, CurrentGrantReader grants,
            CurrentPolicyReader policyReader) {
        this.authorization = new DeferredAuthorization(publicationLock, directRoles, grants);
        this.policies = new DeferredPolicyReader(policyReader);
    }

    @Override
    public void requireCurrent(Connection connection, String principal, DeferredAuthorizationScope scope,
            PolicyObserver observer) throws SQLException {
        authorization.requireCurrent(connection, principal, scope, observer::observe);
    }

    @Override
    public PolicySnapshot observePolicies(Connection connection, String principal, Set<String> roles,
            TablePermission permission, RowProjection rows, MaskProjection masks) throws SQLException {
        return policies.observe(connection, principal, roles, permission, rows::apply, masks::apply);
    }
}
