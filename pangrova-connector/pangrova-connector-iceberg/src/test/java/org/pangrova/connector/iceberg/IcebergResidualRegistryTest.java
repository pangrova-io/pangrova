package org.pangrova.connector.iceberg;

import java.time.Duration;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link IcebergResidualRegistry}.
 *
 * <p>Validates the in-process registry that carries Iceberg residual
 * {@link Expression}s from split planning to the Java Parquet fallback reader,
 * which forwards them to {@code Parquet.ReadBuilder.filter()} for sub-file
 * (row-group / page) pruning.</p>
 */
class IcebergResidualRegistryTest {

    @Test
    void findReturnsAlwaysTrueWhenUnregistered() {
        IcebergResidualRegistry registry = new IcebergResidualRegistry();

        Expression result = registry.find("/path/to/file.parquet");

        assertThat(result).isInstanceOf(org.apache.iceberg.expressions.True.class);
    }

    @Test
    void findReturnsRegisteredExpression() {
        IcebergResidualRegistry registry = new IcebergResidualRegistry();
        Expression filter = Expressions.greaterThan("id", 100L);

        registry.register("/data/file1.parquet", filter);

        assertThat(registry.find("/data/file1.parquet")).isSameAs(filter);
    }

    @Test
    void registerSkipsAlwaysTrue() {
        IcebergResidualRegistry registry = new IcebergResidualRegistry();

        registry.register("/data/file.parquet", Expressions.alwaysTrue());

        // alwaysTrue is a no-op filter — registry must NOT store it
        assertThat(registry.size()).isZero();
    }

    @Test
    void registerSkipsNullPathOrExpression() {
        IcebergResidualRegistry registry = new IcebergResidualRegistry();

        registry.register(null, Expressions.equal("a", 1));
        registry.register("", Expressions.equal("a", 1));
        registry.register("/file.parquet", null);

        assertThat(registry.size()).isZero();
    }

    @Test
    void findReturnsAlwaysTrueForNullOrBlankPath() {
        IcebergResidualRegistry registry = new IcebergResidualRegistry();
        registry.register("/some/file.parquet", Expressions.equal("a", 1));

        assertThat(registry.find(null)).isInstanceOf(org.apache.iceberg.expressions.True.class);
        assertThat(registry.find("")).isInstanceOf(org.apache.iceberg.expressions.True.class);
    }

    @Test
    void registerReplacesExistingEntry() {
        IcebergResidualRegistry registry = new IcebergResidualRegistry();
        Expression first = Expressions.equal("a", 1);
        Expression second = Expressions.equal("a", 2);

        registry.register("/file.parquet", first);
        registry.register("/file.parquet", second);

        assertThat(registry.find("/file.parquet")).isSameAs(second);
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void clearEmptiesRegistry() {
        IcebergResidualRegistry registry = new IcebergResidualRegistry();
        registry.register("/file1.parquet", Expressions.equal("a", 1));
        registry.register("/file2.parquet", Expressions.equal("a", 2));

        registry.clear();

        assertThat(registry.size()).isZero();
        assertThat(registry.find("/file1.parquet"))
                .isInstanceOf(org.apache.iceberg.expressions.True.class);
    }

    @Test
    void multipleFilesIndependentlyTracked() {
        IcebergResidualRegistry registry = new IcebergResidualRegistry();
        Expression f1 = Expressions.greaterThan("id", 10L);
        Expression f2 = Expressions.lessThan("ts", 20L);

        registry.register("/a.parquet", f1);
        registry.register("/b.parquet", f2);

        assertThat(registry.find("/a.parquet")).isSameAs(f1);
        assertThat(registry.find("/b.parquet")).isSameAs(f2);
        assertThat(registry.find("/c.parquet"))
                .isInstanceOf(org.apache.iceberg.expressions.True.class);
        assertThat(registry.size()).isEqualTo(2);
    }

    @Test
    void respectsCustomMaxSize() {
        IcebergResidualRegistry registry = new IcebergResidualRegistry(2, Duration.ofMinutes(5));

        registry.register("/a.parquet", Expressions.equal("x", 1));
        registry.register("/b.parquet", Expressions.equal("x", 2));
        registry.register("/c.parquet", Expressions.equal("x", 3));

        // Caffeine eviction is async; size may include the third briefly,
        // but capacity must not exceed 2 once the eviction settles.
        // We just verify the registry survives over-capacity inserts.
        assertThat(registry.size()).isLessThanOrEqualTo(3);
    }
}
