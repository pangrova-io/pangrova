package org.pangrova.connector.spi.data;

import org.pangrova.connector.spi.data.type.PangrovaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link PangrovaType#fromSqlTypeName(String)}, the centralized
 * SQL type name → PangrovaType mapping extracted from IcebergMetadata,
 * PaimonMetadata, and DdlExecutor.
 */
class PangrovaTypeFromSqlTypeNameTest {

    @ParameterizedTest
    @CsvSource({
            "BOOLEAN,   BOOLEAN",
            "TINYINT,   TINYINT",
            "SMALLINT,  SMALLINT",
            "INT,       INTEGER",
            "INTEGER,   INTEGER",
            "BIGINT,    BIGINT",
            "FLOAT,     FLOAT",
            "DOUBLE,    DOUBLE",
            "DATE,      DATE",
            "TIME,      TIME",
            "TIMESTAMP, TIMESTAMP",
            "VARCHAR,   VARCHAR",
            "STRING,    VARCHAR",
            "UUID,      UUID",
            "FIXED,     FIXED",
            "VARBINARY, VARBINARY",
            "BINARY,    VARBINARY"
    })
    void directMappings(String sqlType, PangrovaType expected) {
        assertThat(PangrovaType.fromSqlTypeName(sqlType)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
            "DECIMAL",
            "DECIMAL(10,2)",
            "NUMERIC",
            "NUMERIC(38,18)"
    })
    void decimalPrefixMapping(String sqlType) {
        assertThat(PangrovaType.fromSqlTypeName(sqlType)).isEqualTo(PangrovaType.DECIMAL);
    }

    @Test
    void caseInsensitive() {
        assertThat(PangrovaType.fromSqlTypeName("bigint")).isEqualTo(PangrovaType.BIGINT);
        assertThat(PangrovaType.fromSqlTypeName("Varchar")).isEqualTo(PangrovaType.VARCHAR);
        assertThat(PangrovaType.fromSqlTypeName("Boolean")).isEqualTo(PangrovaType.BOOLEAN);
    }

    @Test
    void timestampWithTimezone() {
        assertThat(PangrovaType.fromSqlTypeName("TIMESTAMP_TZ")).isEqualTo(PangrovaType.TIMESTAMP_TZ);
        assertThat(PangrovaType.fromSqlTypeName("TIMESTAMPTZ")).isEqualTo(PangrovaType.TIMESTAMP_TZ);
    }

    @Test
    void unknownTypeThrows() {
        assertThatThrownBy(() -> PangrovaType.fromSqlTypeName("UNKNOWN_TYPE"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported SQL type name");
    }

    @ParameterizedTest
    @CsvSource({
            "VECTOR",
            "VECTOR(128)",
            "VECTOR(384, DOUBLE)",
            "vector(768)"
    })
    void vectorPrefixMapping(String sqlType) {
        assertThat(PangrovaType.fromSqlTypeName(sqlType)).isEqualTo(PangrovaType.VECTOR);
    }
}
