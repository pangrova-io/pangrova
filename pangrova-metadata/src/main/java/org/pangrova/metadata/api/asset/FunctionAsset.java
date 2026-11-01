package org.pangrova.metadata.api.asset;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.pangrova.metadata.api.schema.ColumnType;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * {@link AssetType#FUNCTION} asset: a user-defined function (UDF) registered in
 * the catalog.
 *
 * <p>Models scalar, aggregate, and table-valued functions. The {@link Kind}
 * discriminator covers the common cases; planner / executor decide how to
 * dispatch (e.g. JNI for native, classpath load for Java, JIT for SQL).</p>
 *
 * @param catalog       containing catalog
 * @param schema        containing schema
 * @param name          function name
 * @param kind          function kind (scalar, aggregate, table)
 * @param parameters    ordered formal parameters (name + type)
 * @param returnType    return type for scalar/aggregate functions; for table
 *                      functions the row schema is encoded in
 *                      {@code properties} or via a dedicated row-type — this
 *                      field carries the canonical "row of struct" hint and
 *                      may be {@code null} when not applicable
 * @param language      implementation language: {@code JAVA}, {@code SQL},
 *                      {@code PYTHON}, {@code NATIVE}, ...
 * @param body          source body for {@code SQL/PYTHON}, fully-qualified
 *                      class name for {@code JAVA}, native symbol for
 *                      {@code NATIVE}
 * @param deterministic whether repeated calls with the same arguments produce
 *                      the same result (lets the planner constant-fold)
 * @param properties    free-form properties (cost hints, side-effect flags, ...)
 * @param createdAt     creation timestamp
 * @param updatedAt     last-update timestamp
 */
public record FunctionAsset(
        String catalog,
        String schema,
        String name,
        Kind kind,
        List<Parameter> parameters,
        ColumnType returnType,
        String language,
        String body,
        boolean deterministic,
        Map<String, String> properties,
        Instant createdAt,
        Instant updatedAt) implements Asset {

    public FunctionAsset {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(name, "name must not be null");
        checkNotNull(createdAt, "createdAt must not be null");
        checkNotNull(updatedAt, "updatedAt must not be null");
        kind = kind == null ? Kind.SCALAR : kind;
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
        language = language == null ? "" : language;
        body = body == null ? "" : body;
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    @Override
    public AssetType type() {
        return AssetType.FUNCTION;
    }

    /** Function kind discriminator. */
    public enum Kind {
        /** {@code f(scalar...) → scalar} */
        SCALAR,
        /** {@code f(scalar...) → scalar} folded over a multiset (SUM, AVG, ...). */
        AGGREGATE,
        /** {@code f(...) → relation} (returns a row stream). */
        TABLE
    }

    /** Single formal parameter of a function. */
    public record Parameter(String name, ColumnType type) {
        public Parameter {
            checkNotNull(name, "parameter name must not be null");
            checkNotNull(type, "parameter type must not be null");
        }
    }
}
