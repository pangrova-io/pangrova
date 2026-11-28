package org.pangrova.common.units;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DataSize}.
 * Covers constant values, megabytes/gigabytes calculation,
 * and toHumanReadable for each tier (B/KB/MB/GB).
 *
 * <p>Validates: Requirements 4.1-4.4
 */
class DataSizeTest {

    // ── Constants (Requirement 4.2) ──

    @Test
    void kb_equals1024() {
        assertThat(DataSize.KB).isEqualTo(1024L);
    }

    @Test
    void mb_equals1024Squared() {
        assertThat(DataSize.MB).isEqualTo(1024L * 1024);
    }

    @Test
    void gb_equals1024Cubed() {
        assertThat(DataSize.GB).isEqualTo(1024L * 1024 * 1024);
    }

    // ── megabytes(long) (Requirement 4.3) ──

    @Test
    void megabytes_one_returns1048576() {
        assertThat(DataSize.megabytes(1)).isEqualTo(1_048_576L);
    }

    @Test
    void megabytes_256_returnsCorrectValue() {
        assertThat(DataSize.megabytes(256)).isEqualTo(256L * 1024 * 1024);
    }

    // ── gigabytes(long) (Requirement 4.3) ──

    @Test
    void gigabytes_one_returns1073741824() {
        assertThat(DataSize.gigabytes(1)).isEqualTo(1_073_741_824L);
    }

    @Test
    void gigabytes_8_returnsCorrectValue() {
        assertThat(DataSize.gigabytes(8)).isEqualTo(8L * 1024 * 1024 * 1024);
    }

    // ── toHumanReadable — bytes tier (Requirement 4.4) ──

    @Test
    void toHumanReadable_zero_returnsZeroB() {
        assertThat(DataSize.toHumanReadable(0)).isEqualTo("0 B");
    }

    @Test
    void toHumanReadable_500_returns500B() {
        assertThat(DataSize.toHumanReadable(500)).isEqualTo("500 B");
    }

    // ── toHumanReadable — KB tier (Requirement 4.4) ──

    @Test
    void toHumanReadable_1024_returns1KB() {
        assertThat(DataSize.toHumanReadable(1024)).isEqualTo("1.0 KB");
    }

    // ── toHumanReadable — MB tier (Requirement 4.4) ──

    @Test
    void toHumanReadable_megabytes1_returns1MB() {
        assertThat(DataSize.toHumanReadable(DataSize.megabytes(1))).isEqualTo("1.0 MB");
    }

    @Test
    void toHumanReadable_megabytes256_returns256MB() {
        assertThat(DataSize.toHumanReadable(DataSize.megabytes(256))).isEqualTo("256.0 MB");
    }

    // ── toHumanReadable — GB tier (Requirement 4.4) ──

    @Test
    void toHumanReadable_gigabytes1_returns1GB() {
        assertThat(DataSize.toHumanReadable(DataSize.gigabytes(1))).isEqualTo("1.0 GB");
    }

    @Test
    void toHumanReadable_gigabytes2_returns2GB() {
        assertThat(DataSize.toHumanReadable(DataSize.gigabytes(2))).isEqualTo("2.0 GB");
    }

    // ── Private constructor inaccessibility ──

    @Test
    void cannotInstantiate() throws Exception {
        var ctor = DataSize.class.getDeclaredConstructor();
        assertThat(ctor.canAccess(null)).isFalse();
    }
}
