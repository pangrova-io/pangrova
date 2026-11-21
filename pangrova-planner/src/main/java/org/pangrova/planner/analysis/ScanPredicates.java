package org.pangrova.planner.analysis;

import org.pangrova.connector.api.index.predicate.IndexPredicate;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.List;
import java.util.Objects;

/** Scalar predicates attributable to one physical scan in a live plan. */
public record ScanPredicates(QualifiedTableName table, List<IndexPredicate> predicates) {
    public ScanPredicates {
        Objects.requireNonNull(table, "table");
        predicates = List.copyOf(predicates);
    }
}
