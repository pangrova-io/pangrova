package org.pangrova.security.mask;

import org.pangrova.spi.security.UserIdentity;
import org.pangrova.security.authz.ColumnMaskSource;
import org.pangrova.security.authz.DeferredSubjectContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;

/**
 * Evaluates column masking policies for a given user, table, and column.
 * Returns the first applicable mask expression with {column} replaced.
 */
public class ColumnMaskingProcessor implements ColumnMaskSource {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnMaskingProcessor.class);

    private final EnrichedColumnMaskStore store;

    /**
     * Creates a column masking processor.
     *
     * @param store column mask policy store
     */
    public ColumnMaskingProcessor(EnrichedColumnMaskStore store) {
        this.store = store;
    }

    /**
     * Resolves the first applicable column mask for a user.
     *
     * @param user requesting user
     * @param catalog catalog name
     * @param schema schema name
     * @param table table name
     * @param column column name
     * @return rendered mask expression when a policy applies
     */
    public Optional<String> getMask(UserIdentity user, Set<String> effectiveRoles,
                                    String catalog, String schema, String table, String column) {
        return renderMasks(user, effectiveRoles, column, store.getColumnMasks(catalog, schema, table, column));
    }

    /** Uses the same per-column priority and exemptions, including masks outside a query's projection. */
    public boolean hasApplicableMask(UserIdentity user, Set<String> effectiveRoles,
            String catalog, String schema, String table) {
        var byColumn = store.getTableMasks(catalog, schema, table).stream()
                .collect(java.util.stream.Collectors.groupingBy(ColumnMask::columnName));
        return byColumn.entrySet().stream().anyMatch(entry ->
                renderMasks(user, effectiveRoles, entry.getKey(), entry.getValue()).isPresent());
    }

    /** Renders masks read on the caller's current authorization transaction. */
    public static Optional<String> renderMasks(
            UserIdentity user, Set<String> effectiveRoles, String column, java.util.List<ColumnMask> masks) {
        masks.stream().filter(ColumnMask::enabled).forEach(mask ->
                DeferredSubjectContext.requireKnownApplicability(user, effectiveRoles, mask.applyToRoles()));
        return masks.stream()
                .filter(ColumnMask::enabled)
                .sorted(Comparator.comparingLong(ColumnMask::maskId))
                .filter(m -> m.appliesTo(effectiveRoles))
                .findFirst()
                .flatMap(m -> applyMask(m, user, effectiveRoles, column));
    }

    private static Optional<String> applyMask(
            ColumnMask mask, UserIdentity user, Set<String> effectiveRoles, String column) {
        if (mask.isExempt(effectiveRoles)) {
            LOG.info("event=column_mask_exemption principal={} maskId={} column={}",
                    user.principal(), mask.maskId(), column);
            return Optional.empty();
        }
        DeferredSubjectContext.requireStable(user, mask.maskExpression());
        String identifier = '"' + column.replace("\"", "\"\"") + '"';
        return Optional.of(mask.maskExpression().replace("{column}", identifier));
    }
}
