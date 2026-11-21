package org.pangrova.coordinator.testinfra;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.pangrova.persistence.ai.postgresql.embedding.JdbcCurrentModelAuthority;
import org.pangrova.persistence.workflow.postgresql.generation.PostgresGenerationStores;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentDirectRoleReader;

/** Composes real model and artifact authorities for cross-domain consumer tests. */
public final class ModelAuthorityFixtures {
    private static final CurrentModelAuthority MODELS =
            new JdbcCurrentModelAuthority(new JdbcCurrentDirectRoleReader());
    private static final PostgresGenerationStores ARTIFACTS = new PostgresGenerationStores(MODELS, TableWriteAuthorityFixture.create());

    private ModelAuthorityFixtures() {}

    public static CurrentModelAuthority models() {
        return MODELS;
    }

    public static PostgresGenerationStores artifacts() {
        return ARTIFACTS;
    }
}
