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
        List<String> statements = SecuritySchema.loadSchemaStatements();
        // 19 CREATE TABLE + 19 ALTER TABLE + 12 CREATE INDEX + 6 FUNCTION + 9 TRIGGER + 1 INSERT = 66.
        // Tables: 11 policy/revision tables (row_policy, column_mask, security_grants, tenant_quota,
        //         pangrova_roles, pangrova_user_roles, pangrova_group_roles, tenant, tenant_property,
        //         tenant_membership, pangrova_authorization_revision) plus 8 storage/tenant domain
        //         tables (storage_profile_definitions/revisions, file_space_registry,
        //         file_directory_refresh/index/pending, tenants, tenant_user_memberships).
        assertThat(statements).hasSize(66);
        assertThat(statements).anyMatch(s -> s.contains("row_policy_scoped_identity")
                && s.contains("table_id, policy_name"));
        assertThat(statements).anyMatch(s -> s.contains("ALTER TABLE column_mask ADD COLUMN IF NOT EXISTS table_id UUID"));
        assertThat(statements).anyMatch(s -> s.contains("column_mask_scoped_identity")
                && s.contains("mask_name, table_id) NULLS NOT DISTINCT"));
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
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS pangrova_authorization_revision"));
        assertThat(statements).anyMatch(s -> s.contains("ALTER TABLE tenant_quota ADD COLUMN IF NOT EXISTS max_queued_queries"));
        // Storage/tenant domain folded from the pre-split chain.
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS storage_profile_revisions"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS file_space_registry"));
        assertThat(statements).anyMatch(s -> s.contains("CREATE OR REPLACE TRIGGER storage_profile_record_revision"));
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
