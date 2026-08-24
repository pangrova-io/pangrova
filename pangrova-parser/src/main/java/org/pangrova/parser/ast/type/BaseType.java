package org.pangrova.parser.ast.type;

/**
 * Enumeration of primitive SQL base types recognized by the parser.
 * Each value corresponds to a scalar type keyword in the Pangrova SQL grammar.
 */
public enum BaseType {
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    REAL,
    FLOAT,
    DOUBLE,
    VARCHAR,
    CHAR,
    STRING,
    VARBINARY,
    BLOB,
    DATE,
    TIME,
    TIMESTAMP,
    TIMESTAMP_NS,
    INTERVAL,
    UUID,
    VARIANT,
    GEOMETRY,
    GEOGRAPHY
}
