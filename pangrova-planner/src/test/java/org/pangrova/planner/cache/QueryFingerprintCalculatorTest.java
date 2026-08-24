package org.pangrova.planner.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QueryFingerprintCalculatorTest {

    private QueryFingerprintCalculator calculator;

    @BeforeEach
    void setUp() {
        calculator = new QueryFingerprintCalculator();
    }

    @Test
    void sameSqlSameContextProducesSameFingerprint() {
        QueryContext ctx = baseContext();
        QueryFingerprint fp1 = calculator.calculate("SELECT 1", ctx);
        QueryFingerprint fp2 = calculator.calculate("SELECT 1", ctx);
        assertThat(fp1.compositeHash()).isEqualTo(fp2.compositeHash());
    }

    @Test
    void differentTenantIdProducesDifferentFingerprint() {
        QueryContext ctx1 = baseContext();
        QueryContext ctx2 = withTenantId("tenant-B");
        QueryFingerprint fp1 = calculator.calculate("SELECT 1", ctx1);
        QueryFingerprint fp2 = calculator.calculate("SELECT 1", ctx2);
        assertThat(fp1.compositeHash()).isNotEqualTo(fp2.compositeHash());
    }

    @Test
    void differentUserPrincipalProducesDifferentFingerprint() {
        QueryContext ctx1 = baseContext();
        QueryContext ctx2 = withUserPrincipal("bob");
        QueryFingerprint fp1 = calculator.calculate("SELECT 1", ctx1);
        QueryFingerprint fp2 = calculator.calculate("SELECT 1", ctx2);
        assertThat(fp1.compositeHash()).isNotEqualTo(fp2.compositeHash());
    }

    @Test
    void differentRlsPolicyHashProducesDifferentFingerprint() {
        QueryContext ctx1 = baseContext();
        QueryContext ctx2 = withRlsPolicyHash("rls-xyz");
        QueryFingerprint fp1 = calculator.calculate("SELECT 1", ctx1);
        QueryFingerprint fp2 = calculator.calculate("SELECT 1", ctx2);
        assertThat(fp1.compositeHash()).isNotEqualTo(fp2.compositeHash());
    }

    @Test
    void sqlNormalizationIgnoresWhitespaceAndCase() {
        QueryContext ctx = baseContext();
        QueryFingerprint fp1 = calculator.calculate("SELECT  1", ctx);
        QueryFingerprint fp2 = calculator.calculate("select 1", ctx);
        assertThat(fp1.compositeHash()).isEqualTo(fp2.compositeHash());
    }

    private QueryContext baseContext() {
        return new QueryContext(
                "iceberg", "default", "iceberg.default", Map.of("key", "val"),
                "snap-1", "tenant-A", "alice",
                "roles-hash", "rls-abc", "mask-123");
    }

    private QueryContext withTenantId(String tenantId) {
        return new QueryContext(
                "iceberg", "default", "iceberg.default", Map.of("key", "val"),
                "snap-1", tenantId, "alice",
                "roles-hash", "rls-abc", "mask-123");
    }

    private QueryContext withUserPrincipal(String user) {
        return new QueryContext(
                "iceberg", "default", "iceberg.default", Map.of("key", "val"),
                "snap-1", "tenant-A", user,
                "roles-hash", "rls-abc", "mask-123");
    }

    private QueryContext withRlsPolicyHash(String rlsHash) {
        return new QueryContext(
                "iceberg", "default", "iceberg.default", Map.of("key", "val"),
                "snap-1", "tenant-A", "alice",
                "roles-hash", rlsHash, "mask-123");
    }
}
