package org.pangrova.testing.integration.control.ai;

import org.pangrova.persistence.ai.postgresql.embedding.JdbcCurrentModelAuthority;
import org.pangrova.metadata.assembly.generation.GenerationStores;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentDirectRoleReader;

/** Composes real model and artifact authorities for cross-domain consumer tests. */
public final class ModelAuthorityFixtures {
    private static final CurrentModelAuthority MODELS =
            new JdbcCurrentModelAuthority(new JdbcCurrentDirectRoleReader());
    private static final GenerationStores ARTIFACTS = new GenerationStores(MODELS);

    private ModelAuthorityFixtures() {}

    public static CurrentModelAuthority models() {
        return MODELS;
    }

    public static GenerationStores artifacts() {
        return ARTIFACTS;
    }
}
