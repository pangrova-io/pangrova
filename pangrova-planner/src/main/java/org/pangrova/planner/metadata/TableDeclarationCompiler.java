package org.pangrova.planner.metadata;

import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/** Compiles a parsed table declaration, including structural types and generated column metadata. */
public final class TableDeclarationCompiler {
    private TableDeclarationCompiler() {}

    /** Compiles the exact CREATE declaration before any catalog mutation or downstream planning. */
    public static ConnectorTableMetadata compile(
            SchemaTableName tableName, CreateTableStatement stmt) {
        List<ConnectorTableMetadata.Column> typedColumns = stmt.columns().stream()
                .map(col -> new ConnectorTableMetadata.Column(
                        col.name(), DdlTypeMapper.map(col.type()),
                        OptionalInt.empty(),
                        DdlTypeMapper.parameterizedType(col.type())))
                .toList();
        List<String> columnNames = stmt.columns().stream()
                .map(CreateTableStatement.ColumnDefinition::name).toList();
        Map<String, String> props = mergeStructuralProperties(stmt);
        DdlTypeMapper.injectStructuralProperties(stmt.columns(), props);
        GeneratedColumnDefinitionCompiler.compile(stmt.columns(), props);
        return new ConnectorTableMetadata(
                tableName,
                columnNames, typedColumns, props);
    }

    private static Map<String, String> mergeStructuralProperties(CreateTableStatement stmt) {
        Map<String, String> props = new LinkedHashMap<>(stmt.properties());
        if (!stmt.primaryKeyColumns().isEmpty()) {
            props.putIfAbsent("primary-key", String.join(",", stmt.primaryKeyColumns()));
        }
        if (!stmt.partitionColumns().isEmpty()) {
            props.putIfAbsent("partition-by", String.join(",", stmt.partitionColumns()));
        }
        if (!stmt.sortColumns().isEmpty()) {
            List<String> sortEntries = stmt.sortColumns().stream()
                    .map(sc -> sc.columnName() + (sc.ascending() ? ":asc" : ":desc"))
                    .toList();
            props.putIfAbsent("sorted-by", String.join(",", sortEntries));
            if (stmt.useZOrder()) {
                props.putIfAbsent("sort-method", "zorder");
            }
        }
        return props;
    }
}
