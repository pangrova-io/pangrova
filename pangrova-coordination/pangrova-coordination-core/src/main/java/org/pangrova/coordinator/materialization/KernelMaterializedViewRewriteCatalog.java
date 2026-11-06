package org.pangrova.coordinator.materialization;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.port.MaterializationCatalog;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.planner.core.mv.MaterializedViewRewriteCatalog;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.security.spi.authorization.AccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable, authorization-checked source of materialized views for transparent rewrite.
 *
 * <p>Only published materialized views of the authenticated tenant are exposed.
 * A view is skipped when the requester lacks unfiltered SELECT on it, when the
 * view belongs to another schema (its unqualified definition names would bind to
 * the query's schema), or when its generation is older than the configured
 * staleness bound. Rewriting happens only after the requester's policies were
 * applied and only for plans that carried none, so a rewrite never bypasses a
 * row filter or column mask; every candidate is still reader-pinned so GC
 * cannot drop the physical generation while the query reads it. Groups are not
 * carried by {@link ConnectorSession}; the reconstructed identity can only
 * under-approximate a role-derived grant, so a skipped rewrite is the safe
 * outcome.
 */
public final class KernelMaterializedViewRewriteCatalog implements MaterializedViewRewriteCatalog {

    public static final String ENABLED_PROPERTY = "pangrova.mv_rewrite";
    public static final String MAX_STALENESS_PROPERTY = "pangrova.mv_rewrite_max_staleness";
    public static final Duration DEFAULT_MAX_STALENESS = Duration.ofMinutes(30);

    private static final Logger LOG =
            LoggerFactory.getLogger(KernelMaterializedViewRewriteCatalog.class);

    private final MaterializationCatalog catalog;
    private final TenantId tenant;
    private final AccessControl accessControl;
    private final UserIdentity requester;
    private final String defaultCatalog;
    private final String defaultSchema;
    private final Duration maxStaleness;
    private final Instant queryStart;
    private final Consumer<PublishedMaterialization> pinner;

    private KernelMaterializedViewRewriteCatalog(MaterializationCatalog catalog, TenantId tenant,
            AccessControl accessControl, UserIdentity requester, String defaultCatalog,
            String defaultSchema, Duration maxStaleness, Instant queryStart,
            Consumer<PublishedMaterialization> pinner) {
        this.catalog = catalog;
        this.tenant = tenant;
        this.accessControl = accessControl;
        this.requester = requester;
        this.defaultCatalog = defaultCatalog;
        this.defaultSchema = defaultSchema;
        this.maxStaleness = maxStaleness;
        this.queryStart = queryStart;
        this.pinner = pinner;
    }

    /**
     * Creates a rewrite catalog that leases each candidate generation for the query.
     *
     * @param catalog published materialization read model
     * @param tenantId authenticated tenant
     * @param session authenticated connector session
     * @param accessControl authorization policy, or null to disable rewrite
     * @param queryStart planning instant
     * @param pinner leases a candidate's published generation before it is exposed
     * @return rewrite catalog when enabled
     */
    public static Optional<MaterializedViewRewriteCatalog> create(MaterializationCatalog catalog,
            String tenantId, ConnectorSession session, AccessControl accessControl,
            Instant queryStart, Consumer<PublishedMaterialization> pinner) {
        if (catalog == null || accessControl == null || tenantId == null || tenantId.isBlank()) {
            return Optional.empty();
        }
        Map<String, String> properties = session.getProperties();
        if (!enabled(properties)) {
            return Optional.empty();
        }
        String catalogName = session.getCatalog().orElse(null);
        String schemaName = session.getSchema().orElse(null);
        if (catalogName == null || schemaName == null) {
            return Optional.empty();
        }
        String user = session.getUser();
        if (user == null || user.isBlank() || "anonymous".equalsIgnoreCase(user)) {
            return Optional.empty();
        }
        Optional<Duration> staleness = staleness(properties);
        if (staleness.isEmpty()) {
            return Optional.empty();
        }
        UserIdentity identity = new UserIdentity(user, Set.of(), Map.of(), Instant.EPOCH);
        return Optional.of(new KernelMaterializedViewRewriteCatalog(catalog,
                new TenantId(tenantId), accessControl, identity,
                catalogName.toLowerCase(Locale.ROOT), schemaName.toLowerCase(Locale.ROOT),
                staleness.get(), queryStart, java.util.Objects.requireNonNull(pinner, "pinner")));
    }

    @Override
    public List<PublishedView> candidates(Set<String> referencedTables) {
        List<PublishedView> candidates = new ArrayList<>();
        for (PublishedMaterialization published : catalog.listPublished(tenant)) {
            if (published.kind() != MaterializationKind.MATERIALIZED_VIEW) {
                continue;
            }
            List<String> parts = published.object().name().parts();
            if (parts.size() != 3
                    || !parts.get(0).equalsIgnoreCase(defaultCatalog)
                    || !parts.get(1).equalsIgnoreCase(defaultSchema)) {
                continue;
            }
            if (!readsReferencedTable(published, referencedTables)) {
                continue;
            }
            if (!requesterMayRead(parts)) {
                continue;
            }
            if (published.publishedAt().plus(maxStaleness).isBefore(queryStart)) {
                LOG.info("event=materialized_view_rewrite_skipped view={} reason=stale publishedAt={}",
                        published.object().canonicalName(), published.publishedAt());
                continue;
            }
            if (!pin(published)) {
                continue;
            }
            candidates.add(new PublishedView(parts.get(0), parts.get(1), parts.get(2),
                    published.definitionSql(), published.physicalTable().catalog(),
                    published.physicalTable().schema(), published.physicalTable().table(),
                    published.definitionRevision(), published.publishedAt(), maxStaleness));
        }
        return candidates;
    }

    /**
     * Leases the candidate generation, skipping the view when it disappeared
     * between listing and pinning. A failed pin must never fail the query: the
     * original plan still reads the source tables.
     */
    private boolean pin(PublishedMaterialization published) {
        try {
            pinner.accept(published);
            return true;
        } catch (RuntimeException failure) {
            LOG.info("event=materialized_view_rewrite_skipped view={} reason=unpinnable error={}",
                    published.object().canonicalName(), failure.getMessage());
            return false;
        }
    }

    private boolean requesterMayRead(List<String> parts) {
        try {
            accessControl.checkCanReadTableMetadata(requester, parts.get(0), parts.get(1),
                    parts.get(2));
            return true;
        } catch (RuntimeException denied) {
            LOG.info("event=materialized_view_rewrite_skipped view={} reason=not_readable",
                    String.join(".", parts));
            return false;
        }
    }

    private static boolean readsReferencedTable(PublishedMaterialization published,
            Set<String> referencedTables) {
        for (org.pangrova.connector.api.materialization.SourceRef source : published.sources()) {
            String name = (source.catalog() + "." + source.schema() + "." + source.table())
                    .toLowerCase(Locale.ROOT);
            if (referencedTables.contains(name)) {
                return true;
            }
        }
        return published.sources().isEmpty() && referencedTables.contains(
                published.object().canonicalName());
    }

    private static boolean enabled(Map<String, String> properties) {
        String value = properties.get(ENABLED_PROPERTY);
        return value == null || !(value.equalsIgnoreCase("off")
                || value.equalsIgnoreCase("false") || value.equals("0"));
    }

    private static Optional<Duration> staleness(Map<String, String> properties) {
        String value = properties.get(MAX_STALENESS_PROPERTY);
        if (value == null || value.isBlank()) {
            return Optional.of(DEFAULT_MAX_STALENESS);
        }
        Duration parsed;
        try {
            parsed = value.chars().allMatch(Character::isDigit)
                    ? Duration.ofSeconds(Long.parseLong(value))
                    : Duration.parse(value);
        } catch (RuntimeException invalid) {
            LOG.warn("event=materialized_view_rewrite_disabled reason=invalid_max_staleness value={}",
                    value);
            return Optional.empty();
        }
        if (parsed.isNegative() || parsed.isZero()) {
            LOG.warn("event=materialized_view_rewrite_disabled reason=non_positive_max_staleness value={}",
                    value);
            return Optional.empty();
        }
        return Optional.of(parsed);
    }
}
