package org.pangrova.coordinator.query.statement;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.parser.api.ast.statement.AutoEmbeddingColumnSpec;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.type.BaseType;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.type.VectorLogicalType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Validates and compiles generated-column DDL into catalog metadata. */
final class GeneratedColumnDefinitionCompiler {

    private GeneratedColumnDefinitionCompiler() {}

    static void compile(
            List<CreateTableStatement.ColumnDefinition> columns,
            Map<String, String> properties) {
        requireUnmanagedProperties(properties);
        Map<String, CreateTableStatement.ColumnDefinition> columnsByName =
                indexColumnsByName(columns);
        GeneratedColumnSet.Builder definitions = GeneratedColumnSet.newBuilder()
                .setSchemaVersion(GeneratedColumnMetadataCodec.SCHEMA_VERSION);
        for (CreateTableStatement.ColumnDefinition column : columns) {
            column.embeddingSpec().ifPresent(embedding -> {
                validate(column, embedding, columnsByName);
                definitions.addDefinitions(definition(column, embedding));
            });
        }
        if (definitions.getDefinitionsCount() > 0) {
            properties.putAll(GeneratedColumnMetadataCodec.encode(definitions.build()));
        }
    }

    private static void requireUnmanagedProperties(Map<String, String> properties) {
        if (properties.containsKey(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY)
                || properties.containsKey(GeneratedColumnMetadataCodec.SHA256_PROPERTY)) {
            throw validation("generated-column metadata properties are system-managed");
        }
    }

    private static Map<String, CreateTableStatement.ColumnDefinition> indexColumnsByName(
            List<CreateTableStatement.ColumnDefinition> columns) {
        Map<String, CreateTableStatement.ColumnDefinition> result = new HashMap<>();
        for (CreateTableStatement.ColumnDefinition column : columns) {
            String normalized = column.name().toLowerCase(Locale.ROOT);
            if (result.putIfAbsent(normalized, column) != null) {
                throw validation("duplicate column name: " + column.name());
            }
        }
        return result;
    }

    private static void validate(
            CreateTableStatement.ColumnDefinition column,
            AutoEmbeddingColumnSpec embedding,
            Map<String, CreateTableStatement.ColumnDefinition> columnsByName) {
        if (!(column.type() instanceof DataTypeSpec.VectorType vector)) {
            throw validation(
                    "auto-embedding target '" + column.name() + "' must have VECTOR type");
        }
        if (vector.elementType() != org.pangrova.types.VectorType.ElementType.FLOAT) {
            throw validation("auto-embedding target '" + column.name() + "' must have FLOAT32 VECTOR storage");
        }
        if (column.defaultValue().isPresent()) {
            throw validation(
                    "generated column '" + column.name() + "' cannot also declare DEFAULT");
        }
        requireAsynchronousNullable(column, embedding);
        validateSources(column, embedding, columnsByName);
    }

    private static void requireAsynchronousNullable(
            CreateTableStatement.ColumnDefinition column,
            AutoEmbeddingColumnSpec embedding) {
        if (embedding.mode() == AutoEmbeddingColumnSpec.EmbeddingMode.SYNC) {
            throw validation(
                    "SYNC generated column '" + column.name()
                            + "' is unavailable until the Native write relation materializes it "
                            + "in the same atomic catalog commit; use ASYNC");
        }
        if (!column.nullable()) {
            throw validation(
                    "ASYNC generated column '" + column.name()
                            + "' must be nullable until its fenced background publication");
        }
    }

    private static void validateSources(
            CreateTableStatement.ColumnDefinition column,
            AutoEmbeddingColumnSpec embedding,
            Map<String, CreateTableStatement.ColumnDefinition> columnsByName) {
        Set<String> sourceNames = new HashSet<>();
        for (String sourceName : embedding.sourceColumns()) {
            String normalized = sourceName.toLowerCase(Locale.ROOT);
            if (!sourceNames.add(normalized)) {
                throw validation("duplicate source column '" + sourceName
                        + "' for '" + column.name() + "'");
            }
            validateSource(column, sourceName, normalized, columnsByName);
        }
    }

    private static void validateSource(
            CreateTableStatement.ColumnDefinition column,
            String sourceName,
            String normalizedSource,
            Map<String, CreateTableStatement.ColumnDefinition> columnsByName) {
        if (normalizedSource.equals(column.name().toLowerCase(Locale.ROOT))) {
            throw validation(
                    "generated column '" + column.name() + "' cannot reference itself");
        }
        CreateTableStatement.ColumnDefinition source = columnsByName.get(normalizedSource);
        if (source == null) {
            throw validation("source column '" + sourceName + "' does not exist for '"
                    + column.name() + "'");
        }
        if (!isTextSource(source.type())) {
            throw validation("source column '" + sourceName + "' for '" + column.name()
                    + "' must be VARCHAR, CHAR, or STRING; governed FILE embedding "
                    + "is not enabled until a storage gateway is bound");
        }
    }

    private static GeneratedColumnDefinition definition(
            CreateTableStatement.ColumnDefinition column,
            AutoEmbeddingColumnSpec embedding) {
        return GeneratedColumnDefinition.newBuilder()
                .setTargetColumn(column.name())
                .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                .setLogicalModelName(embedding.modelName())
                .addAllSourceColumns(embedding.sourceColumns())
                .setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                .setOutputType(VectorLogicalType.create(((DataTypeSpec.VectorType) column.type()).dimension()))
                .build();
    }

    private static boolean isTextSource(DataTypeSpec type) {
        if (!(type instanceof DataTypeSpec.PrimitiveType primitive)) {
            return false;
        }
        return primitive.baseType() == BaseType.VARCHAR
                || primitive.baseType() == BaseType.CHAR
                || primitive.baseType() == BaseType.STRING;
    }

    private static PangrovaException validation(String message) {
        return new PangrovaException(
                PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
