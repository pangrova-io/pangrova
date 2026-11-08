package org.pangrova.metadata.service;

import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentDirectRoleReader;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentGrantReader;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentPolicyReader;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.persistence.workflow.api.security.TableControlWriteAuthority;
import org.pangrova.persistence.workflow.postgresql.security.PostgresTableControlWriteAuthority;

/** Composition entry point for the Workflow table-control write authority. */
public final class TableControlWriteAuthorities {
    private static final TableControlWriteAuthority POSTGRES =
            new PostgresTableControlWriteAuthority(
                    new MetadataTableControlPolicyEvaluator(
                            AuthorizationTransactionGate::lockPublication,
                            new JdbcCurrentDirectRoleReader(), new JdbcCurrentGrantReader(),
                            new JdbcCurrentPolicyReader()),
                    new MetadataTableControlPolicyProjections());

    private TableControlWriteAuthorities() {}

    public static TableControlWriteAuthority postgres() {
        return POSTGRES;
    }
}
