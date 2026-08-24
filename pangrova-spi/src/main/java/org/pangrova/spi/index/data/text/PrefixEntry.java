package org.pangrova.spi.index.data.text;

import org.pangrova.spi.annotation.Experimental;

/**
 * A single entry in a prefix index: a truncated prefix string and
 * the row offset where it was sampled.
 *
 * @param prefix    the truncated prefix string
 * @param rowOffset the row offset of this entry
 */
@Experimental
public record PrefixEntry(String prefix, long rowOffset) {
}
