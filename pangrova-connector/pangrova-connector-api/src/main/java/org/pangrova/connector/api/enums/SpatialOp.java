package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Spatial operations for R-Tree index evaluation.
 */
@Experimental
public enum SpatialOp {

    /** The indexed region intersects the query region. */
    INTERSECTS,

    /** The indexed region is fully contained within the query region. */
    WITHIN,

    /** The indexed region fully contains the query region. */
    CONTAINS
}
