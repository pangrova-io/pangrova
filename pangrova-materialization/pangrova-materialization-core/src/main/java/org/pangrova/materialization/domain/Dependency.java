package org.pangrova.materialization.domain;

/**
 * Edge of the durable materialization dependency graph.
 *
 * <p>The edge records the downstream definition revision that created it, so a
 * stale graph rebuild cannot silently authorize CASCADE deletion or upstream
 * publication.
 *
 * @param upstream materialization consumed by the downstream definition
 * @param downstream materialization that consumes the upstream object
 * @param downstreamDefinitionRevision definition revision that declared the edge
 * @param kind edge kind
 */
public record Dependency(
        MaterializationId upstream,
        MaterializationId downstream,
        long downstreamDefinitionRevision,
        DependencyKind kind) {

    public Dependency {
        if (upstream == null || downstream == null) {
            throw new IllegalArgumentException("dependency endpoints must not be null");
        }
        if (upstream.equals(downstream)) {
            throw new IllegalArgumentException("a materialization cannot depend on itself");
        }
        if (!upstream.tenant().equals(downstream.tenant())) {
            throw new IllegalArgumentException("cross-tenant dependencies are not allowed");
        }
        if (downstreamDefinitionRevision < 1) {
            throw new IllegalArgumentException("downstream definition revision must be positive");
        }
        if (kind == null) {
            throw new IllegalArgumentException("dependency kind must not be null");
        }
    }
}
