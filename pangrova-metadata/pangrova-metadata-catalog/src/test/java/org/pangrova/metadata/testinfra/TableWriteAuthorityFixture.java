package org.pangrova.metadata.testinfra;

import org.pangrova.metadata.security.MetadataTableControlPolicyProjections;
import org.pangrova.persistence.workflow.postgresql.security.PostgresTableControlWriteAuthority;
import org.pangrova.persistence.workflow.api.security.TableControlWriteAuthority;

/** Composes the PostgreSQL authority with the foreground metadata policy projection. */
public final class TableWriteAuthorityFixture {
    private TableWriteAuthorityFixture() {}

    public static TableControlWriteAuthority create() {
        return new PostgresTableControlWriteAuthority(new MetadataTableControlPolicyProjections());
    }
}
