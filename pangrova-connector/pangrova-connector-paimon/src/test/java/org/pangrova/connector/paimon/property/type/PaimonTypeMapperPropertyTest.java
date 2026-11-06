package org.pangrova.connector.paimon.property.type;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.apache.paimon.types.DataType;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;

import org.pangrova.types.PangrovaType;

/**
 * Property-based tests for PaimonTypeMapper bidirectional type mapping.
 *
 * <p><b>Validates: Requirements 37.1, 37.2, 5.3</b></p>
 */
class PaimonTypeMapperPropertyTest {

    /**
     * PangrovaType values that support a full round-trip through Paimon types.
     * Excludes STRUCT, ARRAY, MAP, UUID, FIXED, INTERVAL_YEAR_MONTH,
     * INTERVAL_DAY_TIME (no simple Paimon equivalent or toPaimonType throws).
     */
    private static final PangrovaType[] ROUND_TRIP_TYPES = {
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
            PangrovaType.VARBINARY
    };

    /**
     * All PangrovaType values that Paimon supports (including those with known promotions).
     * Excludes only types that have no Paimon mapping at all:
     * STRUCT, ARRAY, MAP (complex), INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME.
     */
    private static final PangrovaType[] ALL_PAIMON_SUPPORTED_TYPES = {
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
     * Known type promotions in Paimon round-trip:
     * UUID → VarCharType(36) → VARCHAR (Paimon has no native UUID type)
     * FIXED → BinaryType(16) → VARBINARY (Paimon BinaryType maps back to VARBINARY)
     */
    private static final Map<PangrovaType, PangrovaType> KNOWN_PROMOTIONS = Map.of(
            PangrovaType.UUID, PangrovaType.VARCHAR,
            PangrovaType.FIXED, PangrovaType.VARBINARY
    );

    /**
     * PangrovaType values that have no Paimon mapping (toPaimonType throws).
     */
    private static final Set<PangrovaType> UNSUPPORTED_BY_PAIMON = Set.of(
            PangrovaType.STRUCT,
            PangrovaType.ARRAY,
            PangrovaType.MAP,
            PangrovaType.INTERVAL_YEAR_MONTH,
            PangrovaType.INTERVAL_DAY_TIME
    );

    /**
     * For all PangrovaType values that have a bidirectional Paimon mapping,
     * converting to Paimon type and back must yield the original PangrovaType.
     *
     * <p><b>Validates: Requirements 37.1, 37.2</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature_e2e-pipeline-integration")
    @Tag("Property_PaimonTypeMapper_round-trip")
    void roundTrip_toPaimonType_toPangrovaType_isIdentity(
            @ForAll("roundTripPangrovaTypes") PangrovaType original) {

        DataType paimonType = PaimonTypeMapper.toPaimonType(original);
        PangrovaType roundTripped = PaimonTypeMapper.toPangrovaType(paimonType);

        assertThat(roundTripped).isEqualTo(original);
    }

    /**
     * Property 12: Type System Round-Trip (Paimon).
     *
     * <p>For all PangrovaType values supported by Paimon, converting to Paimon type
     * and back must yield either the original type (identity) or a known promoted
     * type (e.g., UUID → VARCHAR, FIXED → VARBINARY). This verifies the type
     * system mapping is consistent and no information is silently lost.</p>
     *
     * <p><b>Validates: Requirements 5.3</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature_full-pipeline-audit-fix")
    @Tag("Property_12_TypeSystem_RoundTrip_Paimon")
    void property12_allSupportedTypes_roundTripConsistent(
            @ForAll("allPaimonSupportedTypes") PangrovaType original) {

        DataType paimonType = PaimonTypeMapper.toPaimonType(original);
        assertThat(paimonType).as("toPaimonType(%s) must not return null", original).isNotNull();

        PangrovaType roundTripped = PaimonTypeMapper.toPangrovaType(paimonType);
        PangrovaType expected = KNOWN_PROMOTIONS.getOrDefault(original, original);

        assertThat(roundTripped)
                .as("round-trip for %s: expected %s but got %s", original, expected, roundTripped)
                .isEqualTo(expected);
    }

    /**
     * Property 12 (supplementary): Unsupported types must be rejected.
     *
     * <p>PangrovaType values with no Paimon equivalent must throw
     * IllegalArgumentException from toPaimonType, ensuring no silent data loss.</p>
     *
     * <p><b>Validates: Requirements 5.3</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature_full-pipeline-audit-fix")
    @Tag("Property_12_TypeSystem_RoundTrip_Paimon_Unsupported")
    void property12_unsupportedTypes_throwOnConversion(
            @ForAll("unsupportedPaimonTypes") PangrovaType unsupported) {

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> PaimonTypeMapper.toPaimonType(unsupported)
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Provide
    Arbitrary<PangrovaType> roundTripPangrovaTypes() {
        return Arbitraries.of(ROUND_TRIP_TYPES);
    }

    @Provide
    Arbitrary<PangrovaType> allPaimonSupportedTypes() {
        return Arbitraries.of(ALL_PAIMON_SUPPORTED_TYPES);
    }

    @Provide
    Arbitrary<PangrovaType> unsupportedPaimonTypes() {
        return Arbitraries.of(UNSUPPORTED_BY_PAIMON.toArray(PangrovaType[]::new));
    }
}
