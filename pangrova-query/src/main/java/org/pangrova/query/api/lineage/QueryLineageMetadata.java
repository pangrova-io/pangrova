package org.pangrova.query.api.lineage;

import org.pangrova.governance.api.enums.ColumnDerivationKind;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Objects;

/** Immutable logical dataset lineage captured from the authorized query plan. */
@Experimental
public record QueryLineageMetadata(List<Dataset> inputs, List<Dataset> outputs) {
    public QueryLineageMetadata {
        inputs = List.copyOf(inputs);
        outputs = List.copyOf(outputs);
    }

    public static QueryLineageMetadata empty() {
        return new QueryLineageMetadata(List.of(), List.of());
    }

    public record Dataset(String namespace, String name, List<Column> columns) {
        public Dataset {
            Objects.requireNonNull(namespace, "namespace");
            Objects.requireNonNull(name, "name");
            columns = List.copyOf(columns);
        }
    }

    public record Column(String name, List<SourceColumn> sources, ColumnDerivationKind derivation) {
        public Column {
            Objects.requireNonNull(name, "name");
            sources = List.copyOf(sources);
            Objects.requireNonNull(derivation, "derivation");
        }
    }

    public record SourceColumn(String namespace, String dataset, String column) {
        public SourceColumn {
            Objects.requireNonNull(namespace, "namespace");
            Objects.requireNonNull(dataset, "dataset");
            Objects.requireNonNull(column, "column");
        }
    }
}
