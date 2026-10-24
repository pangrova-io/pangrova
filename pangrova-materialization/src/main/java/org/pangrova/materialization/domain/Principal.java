package org.pangrova.materialization.domain;

/**
 * Authenticated principal on whose behalf a materialization operation runs.
 *
 * <p>The domain keeps only the stable principal name. Authentication and
 * authorization adapters map their richer identity types to this value.
 *
 * @param name principal name
 */
public record Principal(String name) {

    public Principal {
        name = Require.text(name, "principal");
    }
}
