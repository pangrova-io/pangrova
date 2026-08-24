package org.pangrova.security.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Verifies the SI-4 schema resource loads and parses into executable statements, and that role
 * encoding round-trips. Live JDBC behavior of the concrete stores is covered by integration tests
 * (testcontainers) in CI.
 */
class PersistentStoreSupportTest {

    @Test
    void schemaResourceParsesIntoExpectedStatements() {
        List<String> statements = PersistentStoreSupport.loadSchemaStatements();
        // 10 CREATE TABLE + 6 CREATE INDEX + 6 ALTER TABLE = 22 total.
        // Tables: row_policy, column_mask, security_grants, tenant_quota, pangrova_roles,
        //         pangrova_user_roles, pangrova_group_roles (F-SEC-04), tenant, tenant_property,
        //         tenant_membership (F-SEC-02).
        // Alters: 6 ADD COLUMN on tenant_quota (F-SEC-02).
        assertThat(statements).hasSize(22);
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS row_policy"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS column_mask"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS security_grants"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS tenant_quota"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS pangrova_roles"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS pangrova_user_roles"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS pangrova_group_roles"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS tenant"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS tenant_property"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS tenant_membership"));
        assertThat(statements).anyMatch(s -> s.contains("ALTER TABLE tenant_quota ADD COLUMN IF NOT EXISTS max_queued_queries"));
        assertThat(statements).allMatch(s -> !s.endsWith(";"));
    }

    @Test
    void rolesRoundTripThroughCommaSeparatedForm() {
        assertThat(PersistentStoreSupport.encodeRoles(List.of())).isEmpty();
        assertThat(PersistentStoreSupport.encodeRoles(List.of("a", "b"))).isEqualTo("a,b");
        assertThat(PersistentStoreSupport.decodeRoles("")).isEmpty();
        assertThat(PersistentStoreSupport.decodeRoles("a,b")).containsExactly("a", "b");
    }
}
