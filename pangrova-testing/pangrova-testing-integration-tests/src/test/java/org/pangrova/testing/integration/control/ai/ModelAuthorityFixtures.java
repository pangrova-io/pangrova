package org.pangrova.testing.integration.control.ai;

import org.pangrova.ai.control.store.jdbc.embedding.JdbcCurrentModelAuthority;
import org.pangrova.metadata.assembly.generation.GenerationStores;
import org.pangrova.security.spi.jdbc.CurrentModelAuthority;
import org.pangrova.security.store.jdbc.JdbcCurrentDirectRoleReader;

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
