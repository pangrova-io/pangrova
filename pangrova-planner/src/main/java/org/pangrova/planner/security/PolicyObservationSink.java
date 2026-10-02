package org.pangrova.planner.security;

import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import java.util.Optional;

/** Records the exact policy values consumed while building one protected plan. */
public interface PolicyObservationSink {

    /** Records a table filter, including an explicitly observed absent policy. */
    void rowFilter(QualifiedTableName table, Optional<String> expression);

    /** Records a column mask, including an explicitly observed absent policy. */
    void columnMask(QualifiedTableName table, String column, Optional<String> expression);
}
