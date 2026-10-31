package org.pangrova.planner;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.pangrova.parser.ast.AllColumns;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.SystemTableReference;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.connector.spi.metadata.MetadataTableListing;

/** Typed utility expansion; names and values never round-trip through generated SQL. */
public record MetadataTableQuery(List<String> baseTable, MetadataTableListing listing) {
    public MetadataTableQuery {
        baseTable = List.copyOf(baseTable);
        if (baseTable.isEmpty() || baseTable.size() > 3 || baseTable.stream().anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException("Metadata query requires a qualified base table");
        }
        Objects.requireNonNull(listing, "listing");
    }

    Query toAst() {
        Expression predicate = null;
        for (var filter : listing.equalityFilters().entrySet()) {
            Expression equality = new BinaryExpression(Identifier.of(filter.getKey()),
                    BinaryExpression.Operator.EQUAL, Literal.ofString(filter.getValue()));
            predicate = predicate == null ? equality : new BinaryExpression(
                    predicate, BinaryExpression.Operator.AND, equality);
        }
        var source = TableRef.systemTable(new SystemTableReference(
                new Identifier(baseTable), listing.source().suffix()), Optional.empty(), Optional.empty());
        return new Query(false, List.of(new AllColumns()), List.of(source), Optional.ofNullable(predicate),
                List.of(), Optional.empty(), List.of(), Optional.empty());
    }
}
