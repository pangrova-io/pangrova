package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;

import java.util.List;

import java.util.Objects;

/**
 * Connector-level definition of a user-defined function.
 *
 * <p>Carries enough information for the engine to resolve, type-check, and
 * dispatch the function. The representation is intentionally connector-agnostic:
 * connectors that support function registration (Iceberg REST catalog, Hive
 * Metastore, Paimon) convert to/from their native model.</p>
 *
 * @param name          schema-qualified function name
 * @param kind          function kind (SCALAR, AGGREGATE, TABLE)
 * @param parameters    ordered formal parameters
 * @param returnType    return type name (e.g. "BIGINT", "VARCHAR"); may be
 *                      {@code ""} for table functions whose output schema is
 *                      encoded in parameters or properties
 * @param language      implementation language: JAVA, SQL, PYTHON, NATIVE
 * @param body          source body for SQL/PYTHON, fully-qualified class name
 *                      for JAVA, native symbol for NATIVE
 * @param deterministic whether repeated calls with the same arguments produce
 *                      the same result
 */
@Experimental
public record ConnectorFunctionDefinition(
        SchemaTableName name,
        Kind kind,
        List<Parameter> parameters,
        String returnType,
        String language,
        String body,
        boolean deterministic) {

    public ConnectorFunctionDefinition {
        Objects.requireNonNull(name, "name must not be null");
        kind = kind == null ? Kind.SCALAR : kind;
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
        returnType = returnType == null ? "" : returnType;
        language = language == null ? "" : language;
        body = body == null ? "" : body;
    }

    /** Function kind discriminator. */
    public enum Kind {
        /** f(scalar...) -> scalar */
        SCALAR,
        /** f(scalar...) -> scalar folded over a multiset (SUM, AVG, ...). */
        AGGREGATE,
        /** f(...) -> relation (returns a row stream). */
        TABLE
    }

    /** Single formal parameter of a function. */
    public record Parameter(String name, String type) {
        public Parameter {
            Objects.requireNonNull(name, "parameter name must not be null");
            Objects.requireNonNull(type, "parameter type must not be null");
        }
    }
}
