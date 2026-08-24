package org.pangrova.security.mask;

import org.pangrova.spi.security.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Optional;

/**
 * Evaluates column masking policies for a given user, table, and column.
 * Returns the first applicable mask expression with {column} replaced.
 */
public class ColumnMaskingProcessor {

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
    public Optional<String> getMask(UserIdentity user, String catalog, String schema, String table, String column) {
        return store.getColumnMasks(catalog, schema, table, column).stream()
                .filter(ColumnMask::enabled)
                .sorted(Comparator.comparingLong(ColumnMask::maskId))
                .filter(m -> m.appliesTo(user))
                .findFirst()
                .flatMap(m -> applyMask(m, user, column));
    }

    private Optional<String> applyMask(ColumnMask mask, UserIdentity user, String column) {
        if (mask.isExempt(user)) {
            LOG.info("event=column_mask_exemption principal={} maskId={} column={}",
                    user.principal(), mask.maskId(), column);
            return Optional.empty();
        }
        return Optional.of(mask.maskExpression().replace("{column}", column));
    }
}
