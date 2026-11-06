package org.pangrova.connector.api.metadata.table;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * A connector-owned read-only view definition over explicitly frozen inputs.
 * SQL refers only to input aliases in this connector's catalog, never interpolated
 * physical table names. It must retain a frozen source for authorization.
 * The Planner validates the query, preserves input identities and applies normal
 * authorization before optimization; the connector supplies no result rows.
 */
@Experimental
public record ConnectorDerivedQuery(String sql, List<Input> inputs) {
    /** A definition-local alias with its exact selected source and declaration. */
    public record Input(String alias, ConnectorReadBinding binding) {
        public Input {
            Objects.requireNonNull(alias, "alias");
            Objects.requireNonNull(binding, "binding");
            if (alias.isBlank()) throw new IllegalArgumentException("Derived input alias must not be blank");
            if (binding.state() == ConnectorReadBinding.SnapshotState.UNVERSIONED
                    || binding.sourceIdentity().isBlank()) {
                throw new IllegalArgumentException("Derived input requires a frozen source identity");
            }
            binding.declaration().requireExactDeclaration();
        }
    }

    public ConnectorDerivedQuery {
        Objects.requireNonNull(sql, "sql");
        inputs = List.copyOf(Objects.requireNonNull(inputs, "inputs"));
        if (sql.isBlank() || inputs.isEmpty()) {
            throw new IllegalArgumentException("Derived query requires SQL and frozen inputs");
        }
        var aliases = new HashSet<String>();
        for (var input : inputs) {
            if (!aliases.add(input.alias().toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Ambiguous derived input alias: " + input.alias());
            }
        }
    }

    /** Omit definition text and opaque source handles from diagnostics. */
    @Override
    public String toString() {
        return "ConnectorDerivedQuery[inputs=" + inputs.stream().map(Input::alias).toList() + "]";
    }
}
