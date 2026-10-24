package org.pangrova.materialization.domain;

import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.pangrova.spi.materialization.SourceRef;

/**
 * Immutable definition revision of a materialization.
 *
 * <p>Any semantic change to the definition creates a new revision; existing
 * revisions are never updated or deleted. The revision fingerprint binds the
 * SQL, its bound plan, output schema, source set, dependency set, policy
 * snapshot, security mode, and definer.
 *
 * @param object owning object
 * @param revision monotonically increasing revision, starting at one
 * @param sql definition SQL as submitted
 * @param binding normalized binding produced by the planner adapter
 * @param outputSchema output schema
 * @param sources source tables read by the definition
 * @param dependencies materialization objects consumed by the definition
 * @param policySnapshot source policy fingerprints captured at planning time
 * @param securityMode execution security mode
 * @param definer persisted definer principal
 * @param sessionProperties planning session properties bound at definition time
 * @param createdAt creation instant
 */
public record DefinitionRevision(
        MaterializationId object,
        long revision,
        String sql,
        Binding binding,
        OutputSchema outputSchema,
        Set<SourceRef> sources,
        Set<Dependency> dependencies,
        PolicySnapshot policySnapshot,
        SecurityMode securityMode,
        Principal definer,
        Map<String, String> sessionProperties,
        Instant createdAt) {

    public DefinitionRevision {
        if (object == null) {
            throw new IllegalArgumentException("object must not be null");
        }
        if (revision < 1) {
            throw new IllegalArgumentException("definition revision must be positive");
        }
        sql = Require.text(sql, "definition sql");
        if (binding == null || outputSchema == null || policySnapshot == null) {
            throw new IllegalArgumentException("binding, outputSchema, and policySnapshot must not be null");
        }
        if (sources == null || dependencies == null) {
            throw new IllegalArgumentException("sources and dependencies must not be null");
        }
        sources = Set.copyOf(sources);
        TreeSet<Dependency> ordered = new TreeSet<>(
                Comparator.comparing((Dependency dependency) -> dependency.upstream().canonicalName())
                        .thenComparing(dependency -> dependency.kind().name()));
        ordered.addAll(dependencies);
        for (Dependency dependency : ordered) {
            if (!object.equals(dependency.downstream())) {
                throw new IllegalArgumentException("dependency downstream must be the owning object");
            }
            if (dependency.downstreamDefinitionRevision() != revision) {
                throw new IllegalArgumentException(
                        "dependency revision must match the definition revision");
            }
        }
        dependencies = Set.copyOf(ordered);
        if (!policySnapshot.fingerprints().keySet().equals(sources)) {
            throw new IllegalArgumentException(
                    "policy snapshot must contain exactly one entry per source");
        }
        if (securityMode == null) {
            throw new IllegalArgumentException("securityMode must not be null");
        }
        if (!securityMode.isSupported()) {
            throw MaterializationException.invalidDefinition(
                    "unsupported security mode: " + securityMode);
        }
        if (definer == null || createdAt == null) {
            throw new IllegalArgumentException("definer and createdAt must not be null");
        }
        sessionProperties = sessionProperties == null
                ? Map.of() : Map.copyOf(new TreeMap<>(sessionProperties));
    }

    /**
     * Returns the canonical SHA-256 fingerprint of this revision.
     *
     * @return lowercase hex fingerprint
     */
    public String fingerprint() {
        StringBuilder canonical = new StringBuilder("definition:")
                .append(object.canonicalName())
                .append(':')
                .append(revision)
                .append('|')
                .append(sql)
                .append('|')
                .append(binding.canonicalSql())
                .append('|')
                .append(binding.planFingerprint())
                .append('|')
                .append(outputSchema.fingerprint())
                .append('|')
                .append(policySnapshot.fingerprint())
                .append('|')
                .append(securityMode)
                .append('|')
                .append(definer.name());
        sessionProperties.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(property -> canonical.append("|prop:")
                        .append(property.getKey()).append('=')
                        .append(property.getValue()));
        sources.stream()
                .sorted(Comparator.comparing(SourceRef::canonicalName))
                .forEach(source -> canonical.append("|src:").append(source.canonicalName()));
        dependencies.stream()
                .sorted(Comparator.comparing((Dependency dependency) -> dependency.upstream().canonicalName())
                        .thenComparing(dependency -> dependency.kind().name()))
                .forEach(dependency -> canonical.append("|dep:")
                        .append(dependency.upstream().canonicalName())
                        .append(':')
                        .append(dependency.kind()));
        return CanonicalHash.sha256(canonical.toString());
    }

    /**
     * Returns whether another revision has identical materialized semantics.
     *
     * @param other other revision
     * @return true when fingerprints match
     */
    public boolean sameSemantics(DefinitionRevision other) {
        return other != null && fingerprint().equals(other.fingerprint());
    }
}
