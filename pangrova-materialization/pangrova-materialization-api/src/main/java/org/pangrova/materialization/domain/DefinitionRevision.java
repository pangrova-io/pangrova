package org.pangrova.materialization.domain;

import org.pangrova.common.util.DigestUtils;

import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.pangrova.connector.api.materialization.SourceRef;

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
 * @param definerGroups identity groups bound to the definer when the revision was planned
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
        Set<String> definerGroups,
        Map<String, String> sessionProperties,
        Instant createdAt) {

    private static final Comparator<Dependency> DEPENDENCY_ORDER =
            Comparator.comparing((Dependency dependency) -> dependency.upstream().canonicalName())
                    .thenComparing(dependency -> dependency.kind().name());

    /** Creates a revision whose definer carries no group identity. */
    public DefinitionRevision(MaterializationId object, long revision, String sql, Binding binding,
            OutputSchema outputSchema, Set<SourceRef> sources, Set<Dependency> dependencies,
            PolicySnapshot policySnapshot, SecurityMode securityMode, Principal definer,
            Map<String, String> sessionProperties, Instant createdAt) {
        this(object, revision, sql, binding, outputSchema, sources, dependencies, policySnapshot,
                securityMode, definer, Set.of(), sessionProperties, createdAt);
    }

    public DefinitionRevision {
        requireIdentity(object, revision);
        sql = Require.text(sql, "definition sql");
        requirePayload(binding, outputSchema, sources, dependencies, policySnapshot);
        sources = Set.copyOf(sources);
        dependencies = orderedDependencies(object, revision, dependencies);
        requirePolicyCoverage(policySnapshot, sources);
        requireSupportedSecurity(securityMode);
        if (definer == null || createdAt == null) {
            throw new IllegalArgumentException("definer and createdAt must not be null");
        }
        definerGroups = definerGroups == null
                ? Set.of() : Set.copyOf(new TreeSet<>(definerGroups));
        sessionProperties = sessionProperties == null
                ? Map.of() : Map.copyOf(new TreeMap<>(sessionProperties));
    }

    private static void requireIdentity(MaterializationId object, long revision) {
        if (object == null) {
            throw new IllegalArgumentException("object must not be null");
        }
        if (revision < 1) {
            throw new IllegalArgumentException("definition revision must be positive");
        }
    }

    private static void requirePayload(Binding binding, OutputSchema outputSchema,
            Set<SourceRef> sources, Set<Dependency> dependencies, PolicySnapshot policySnapshot) {
        if (binding == null || outputSchema == null || policySnapshot == null) {
            throw new IllegalArgumentException(
                    "binding, outputSchema, and policySnapshot must not be null");
        }
        if (sources == null || dependencies == null) {
            throw new IllegalArgumentException("sources and dependencies must not be null");
        }
    }

    private static Set<Dependency> orderedDependencies(MaterializationId object, long revision,
            Set<Dependency> dependencies) {
        TreeSet<Dependency> ordered = new TreeSet<>(DEPENDENCY_ORDER);
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
        return Set.copyOf(ordered);
    }

    private static void requirePolicyCoverage(PolicySnapshot policySnapshot,
            Set<SourceRef> sources) {
        if (!policySnapshot.fingerprints().keySet().equals(sources)) {
            throw new IllegalArgumentException(
                    "policy snapshot must contain exactly one entry per source");
        }
    }

    private static void requireSupportedSecurity(SecurityMode securityMode) {
        if (securityMode == null) {
            throw new IllegalArgumentException("securityMode must not be null");
        }
        if (!securityMode.isSupported()) {
            throw MaterializationException.invalidDefinition(
                    "unsupported security mode: " + securityMode);
        }
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
        definerGroups.stream().sorted()
                .forEach(group -> canonical.append("|group:").append(group));
        sessionProperties.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(property -> canonical.append("|prop:")
                        .append(property.getKey()).append('=')
                        .append(property.getValue()));
        sources.stream()
                .sorted(Comparator.comparing(SourceRef::canonicalName))
                .forEach(source -> canonical.append("|src:").append(source.canonicalName()));
        dependencies.stream()
                .sorted(DEPENDENCY_ORDER)
                .forEach(dependency -> canonical.append("|dep:")
                        .append(dependency.upstream().canonicalName())
                        .append(':')
                        .append(dependency.kind()));
        return DigestUtils.sha256Hex(canonical.toString());
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
