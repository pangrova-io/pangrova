package org.pangrova.connector.iceberg.property.type;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.connector.iceberg.type.IcebergTypeMapper;
import org.apache.iceberg.types.Type;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;

import org.pangrova.spi.connector.data.type.PangrovaType;

/**
 * Property-based tests for IcebergTypeMapper bidirectional type mapping.
 *
 * <p><b>Validates: Requirements 36.1, 36.2, 5.3</b></p>
 */
class IcebergTypeMapperPropertyTest {

    /**
     * PangrovaType values that support a full round-trip through Iceberg types.
     * Excludes STRUCT, ARRAY, MAP (complex types without simple toIcebergType mapping)
     * and TINYINT, SMALLINT, INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME (no Iceberg equivalent).
     */
    private static final PangrovaType[] ROUND_TRIP_TYPES = {
            PangrovaType.BOOLEAN,
            PangrovaType.INTEGER,
            PangrovaType.BIGINT,
            PangrovaType.FLOAT,
            PangrovaType.DOUBLE,
            PangrovaType.DECIMAL,
            PangrovaType.DATE,
            PangrovaType.TIME,
            PangrovaType.TIMESTAMP,
            PangrovaType.TIMESTAMP_TZ,
            PangrovaType.VARCHAR,
            PangrovaType.UUID,
            PangrovaType.FIXED,
            PangrovaType.VARBINARY
    };

    /**
     * All PangrovaType values that Iceberg supports (including those with known promotions).
     * Excludes only types that have no Iceberg mapping at all:
     * STRUCT, ARRAY, MAP (complex), INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME.
     */
    private static final PangrovaType[] ALL_ICEBERG_SUPPORTED_TYPES = {
            PangrovaType.BOOLEAN,
            PangrovaType.TINYINT,
            PangrovaType.SMALLINT,
            PangrovaType.INTEGER,
            PangrovaType.BIGINT,
            PangrovaType.FLOAT,
            PangrovaType.DOUBLE,
            PangrovaType.DECIMAL,
            PangrovaType.DATE,
            PangrovaType.TIME,
            PangrovaType.TIMESTAMP,
            PangrovaType.TIMESTAMP_TZ,
            PangrovaType.VARCHAR,
            PangrovaType.UUID,
            PangrovaType.FIXED,
            PangrovaType.VARBINARY
    };

    /**
     * Known type promotions: Iceberg lacks TINYINT/SMALLINT, so they widen to INTEGER.
     */
    private static final Map<PangrovaType, PangrovaType> KNOWN_PROMOTIONS = Map.of(
            PangrovaType.TINYINT, PangrovaType.INTEGER,
            PangrovaType.SMALLINT, PangrovaType.INTEGER
    );

    /**
     * PangrovaType values that have no Iceberg mapping (toIcebergType throws).
     * VECTOR is included because it requires the parameterized toIcebergVectorType method
     * (dimension info is needed), so the simple toIcebergType(PangrovaType) rejects it.
     */
    private static final Set<PangrovaType> UNSUPPORTED_BY_ICEBERG = Set.of(
            PangrovaType.STRUCT,
            PangrovaType.ARRAY,
            PangrovaType.MAP,
            PangrovaType.INTERVAL_YEAR_MONTH,
            PangrovaType.INTERVAL_DAY_TIME,
            PangrovaType.VECTOR
    );

    /**
     * For all PangrovaType values that have a bidirectional Iceberg mapping,
     * converting to Iceberg type and back must yield the original PangrovaType.
     *
     * <p><b>Validates: Requirements 36.1, 36.2</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature_e2e-pipeline-integration")
    @Tag("Property_IcebergTypeMapper_round-trip")
    void roundTrip_toIcebergType_toPangrovaType_isIdentity(
            @ForAll("roundTripPangrovaTypes") PangrovaType original) {

        Type icebergType = IcebergTypeMapper.toIcebergType(original);
        PangrovaType roundTripped = IcebergTypeMapper.toPangrovaType(icebergType);

        assertThat(roundTripped).isEqualTo(original);
    }

    /**
     * Property 12: Type System Round-Trip (Iceberg).
     *
     * <p>For all PangrovaType values supported by Iceberg, converting to Iceberg type
     * and back must yield either the original type (identity) or a known promoted
     * type (e.g., TINYINT → INTEGER). This verifies the type system mapping is
     * consistent and no information is silently lost.</p>
     *
     * <p><b>Validates: Requirements 5.3</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature_full-pipeline-audit-fix")
    @Tag("Property_12_TypeSystem_RoundTrip_Iceberg")
    void property12_allSupportedTypes_roundTripConsistent(
            @ForAll("allIcebergSupportedTypes") PangrovaType original) {

        Type icebergType = IcebergTypeMapper.toIcebergType(original);
        assertThat(icebergType).as("toIcebergType(%s) must not return null", original).isNotNull();

        PangrovaType roundTripped = IcebergTypeMapper.toPangrovaType(icebergType);
        PangrovaType expected = KNOWN_PROMOTIONS.getOrDefault(original, original);

        assertThat(roundTripped)
                .as("round-trip for %s: expected %s but got %s", original, expected, roundTripped)
                .isEqualTo(expected);
    }

    /**
     * Property 12 (supplementary): Unsupported types must be rejected.
     *
     * <p>PangrovaType values with no Iceberg equivalent must throw
     * IllegalArgumentException from toIcebergType, ensuring no silent data loss.</p>
     *
     * <p><b>Validates: Requirements 5.3</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature_full-pipeline-audit-fix")
    @Tag("Property_12_TypeSystem_RoundTrip_Iceberg_Unsupported")
    void property12_unsupportedTypes_throwOnConversion(
            @ForAll("unsupportedIcebergTypes") PangrovaType unsupported) {

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> IcebergTypeMapper.toIcebergType(unsupported)
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Provide
    Arbitrary<PangrovaType> roundTripPangrovaTypes() {
        return Arbitraries.of(ROUND_TRIP_TYPES);
    }

    @Provide
    Arbitrary<PangrovaType> allIcebergSupportedTypes() {
        return Arbitraries.of(ALL_ICEBERG_SUPPORTED_TYPES);
    }

    @Provide
    Arbitrary<PangrovaType> unsupportedIcebergTypes() {
        return Arbitraries.of(UNSUPPORTED_BY_ICEBERG.toArray(PangrovaType[]::new));
    }
}
