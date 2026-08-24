package org.pangrova.planner.property.cache;

import org.pangrova.planner.cache.CacheKey;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link CacheKey} isolation dimensions.
 *
 * <p><b>Validates: Requirements 1.11, 1.12, 2.11, 2.12</b></p>
 */
class CacheKeyPropertyTest {

    // ── Generators ──

    @Provide
    Arbitrary<String> sqls() {
        return Arbitraries.of(
                "SELECT * FROM orders",
                "SELECT count(*) FROM users",
                "SELECT id, name FROM products WHERE price > 10",
                "INSERT INTO logs VALUES (1)",
                "SELECT 1");
    }

    @Provide
    Arbitrary<String> tenantIds() {
        return Arbitraries.of("tenant-a", "tenant-b", "tenant-c", "default");
    }

    @Provide
    Arbitrary<String> principals() {
        return Arbitraries.of("alice", "bob", "charlie", "anonymous");
    }

    @Provide
    Arbitrary<Map<String, String>> sessionProps() {
        return Arbitraries.of(
                Map.of(),
                Map.of("timezone", "UTC"),
                Map.of("timezone", "PST"),
                Map.of("timezone", "UTC", "locale", "en_US"),
                Map.of("catalog", "iceberg", "schema", "public"));
    }

    @Provide
    Arbitrary<String> policyVersions() {
        return Arbitraries.of("v1", "v2", "v3", null);
    }

    record CacheKeyInput(String sql, String tenantId, String principal,
                         Map<String, String> sessionProps, String policyVersion) {
        CacheKey toCacheKey() {
            return CacheKey.of(sql, tenantId, principal, sessionProps, policyVersion);
        }
    }

    @Provide
    Arbitrary<CacheKeyInput> cacheKeyInputs() {
        return Combinators.combine(sqls(), tenantIds(), principals(), sessionProps(), policyVersions())
                .as(CacheKeyInput::new);
    }

    // ── Property 1: Identical contexts produce identical keys ──

    @Property(tries = 10)
    @Label("Property 1: identical context always produces identical CacheKey")
    void identicalContextProducesIdenticalKey(@ForAll("cacheKeyInputs") CacheKeyInput input) {
        CacheKey key1 = input.toCacheKey();
        CacheKey key2 = input.toCacheKey();

        assertThat(key1).isEqualTo(key2);
        assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
        assertThat(key1.keyString()).isEqualTo(key2.keyString());
    }

    // ── Property 2: Different tenants produce different keys ──

    @Property(tries = 10)
    @Label("Property 2: different tenantId produces different CacheKey")
    void differentTenantProducesDifferentKey(
            @ForAll("sqls") String sql,
            @ForAll("principals") String principal,
            @ForAll("sessionProps") Map<String, String> props,
            @ForAll("policyVersions") String policyVersion) {

        CacheKey keyA = CacheKey.of(sql, "tenant-a", principal, props, policyVersion);
        CacheKey keyB = CacheKey.of(sql, "tenant-b", principal, props, policyVersion);

        assertThat(keyA).isNotEqualTo(keyB);
        assertThat(keyA.keyString()).isNotEqualTo(keyB.keyString());
    }

    // ── Property 3: Different principals produce different keys ──

    @Property(tries = 10)
    @Label("Property 3: different principal produces different CacheKey")
    void differentPrincipalProducesDifferentKey(
            @ForAll("sqls") String sql,
            @ForAll("tenantIds") String tenantId,
            @ForAll("sessionProps") Map<String, String> props,
            @ForAll("policyVersions") String policyVersion) {

        CacheKey keyAlice = CacheKey.of(sql, tenantId, "alice", props, policyVersion);
        CacheKey keyBob = CacheKey.of(sql, tenantId, "bob", props, policyVersion);

        assertThat(keyAlice).isNotEqualTo(keyBob);
        assertThat(keyAlice.keyString()).isNotEqualTo(keyBob.keyString());
    }

    // ── Property 4: Different policy versions produce different keys ──

    @Property(tries = 10)
    @Label("Property 4: different policyVersion produces different CacheKey")
    void differentPolicyVersionProducesDifferentKey(
            @ForAll("sqls") String sql,
            @ForAll("tenantIds") String tenantId,
            @ForAll("principals") String principal,
            @ForAll("sessionProps") Map<String, String> props) {

        CacheKey keyV1 = CacheKey.of(sql, tenantId, principal, props, "v1");
        CacheKey keyV2 = CacheKey.of(sql, tenantId, principal, props, "v2");

        assertThat(keyV1).isNotEqualTo(keyV2);
        assertThat(keyV1.keyString()).isNotEqualTo(keyV2.keyString());
    }

    // ── Property 5: Different session properties produce different keys ──

    @Property(tries = 10)
    @Label("Property 5: different sessionProperties produces different CacheKey")
    void differentSessionPropsProducesDifferentKey(
            @ForAll("sqls") String sql,
            @ForAll("tenantIds") String tenantId,
            @ForAll("principals") String principal,
            @ForAll("policyVersions") String policyVersion) {

        CacheKey keyUtc = CacheKey.of(sql, tenantId, principal, Map.of("tz", "UTC"), policyVersion);
        CacheKey keyPst = CacheKey.of(sql, tenantId, principal, Map.of("tz", "PST"), policyVersion);

        assertThat(keyUtc).isNotEqualTo(keyPst);
        assertThat(keyUtc.keyString()).isNotEqualTo(keyPst.keyString());
    }

    // ── Property 6: SQL normalization ensures whitespace-insensitive equality ──

    @Property(tries = 10)
    @Label("Property 6: SQL normalization collapses whitespace and case")
    void sqlNormalizationProducesConsistentKey(
            @ForAll("tenantIds") String tenantId,
            @ForAll("principals") String principal) {

        CacheKey key1 = CacheKey.of("SELECT  *  FROM  orders", tenantId, principal, Map.of(), null);
        CacheKey key2 = CacheKey.of("select * from orders", tenantId, principal, Map.of(), null);

        assertThat(key1).isEqualTo(key2);
        assertThat(key1.keyString()).isEqualTo(key2.keyString());
    }

    // ── Property 7: Session properties order does not affect key ──

    @Property(tries = 10)
    @Label("Property 7: session properties order is irrelevant")
    void sessionPropertiesOrderIrrelevant(
            @ForAll("sqls") String sql,
            @ForAll("tenantIds") String tenantId,
            @ForAll("principals") String principal) {

        Map<String, String> propsA = new TreeMap<>(Map.of("a", "1", "b", "2", "c", "3"));
        Map<String, String> propsB = Map.of("c", "3", "a", "1", "b", "2");

        CacheKey keyA = CacheKey.of(sql, tenantId, principal, propsA, "v1");
        CacheKey keyB = CacheKey.of(sql, tenantId, principal, propsB, "v1");

        assertThat(keyA).isEqualTo(keyB);
        assertThat(keyA.keyString()).isEqualTo(keyB.keyString());
    }
}
