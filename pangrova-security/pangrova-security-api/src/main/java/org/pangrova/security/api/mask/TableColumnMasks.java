package org.pangrova.security.api.mask;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;

/** Column policy values for one subject and table, captured for a single authorization operation. */
@Experimental
@FunctionalInterface
public interface TableColumnMasks {
    /** Renders the selected column using the captured policy values. */
    Optional<String> mask(String column);
}
