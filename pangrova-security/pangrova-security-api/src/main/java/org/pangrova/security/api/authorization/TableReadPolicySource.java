package org.pangrova.security.api.authorization;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.mask.ColumnMaskSource;
import org.pangrova.security.api.mask.TableColumnMasks;
import org.pangrova.security.api.rowfilter.RowFilterSource;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Captures the authority for a table during one synchronous protected read. */
@Internal
@FunctionalInterface
public interface TableReadPolicySource {
    /** Binds row and column policy evaluation to one table and one known subject. */
    PolicyRead bind(UserIdentity user, Set<String> roles, String catalog, String schema, String table);

    /** Evaluates one table under the authority captured for this read. */
    interface PolicyRead {
        /** Returns the applicable row constraint for the bound subject. */
        Optional<String> rowFilter();

        /** Returns the applicable masks by column for the bound subject. */
        TableColumnMasks columnMasks();

        /** Checks all columns, including columns absent from the scan's output. */
        boolean hasApplicableMask();
    }

    /** Composes independent policy sources for standalone and injected authorities. */
    static TableReadPolicySource separate(RowFilterSource filters, ColumnMaskSource masks) {
        Objects.requireNonNull(filters, "filters");
        Objects.requireNonNull(masks, "masks");
        return (user, roles, catalog, schema, table) -> new PolicyRead() {
            @Override
            public Optional<String> rowFilter() {
                return filters.getFilter(user, roles, catalog, schema, table);
            }

            @Override
            public TableColumnMasks columnMasks() {
                return masks.getMasks(user, roles, catalog, schema, table);
            }

            @Override
            public boolean hasApplicableMask() {
                return masks.hasApplicableMask(user, roles, catalog, schema, table);
            }
        };
    }
}
