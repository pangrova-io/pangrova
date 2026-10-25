package org.pangrova.security.mask;

import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.DeferredSubjectContext;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates: Requirements 18.8 — ColumnMaskingProcessor evaluates masks with role and exemption logic.
 */
class ColumnMaskingProcessorTest {

    private static final UserIdentity ANALYST = new UserIdentity(
            "alice", Set.of("analyst"), Map.of(), Instant.now());
    private static final UserIdentity ADMIN = new UserIdentity(
            "bob", Set.of("admin"), Map.of(), Instant.now());

    @ParameterizedTest
    @ValueSource(strings = {"current_groups()", "CURRENT_GROUPS ( )",
            "user_attribute('department')", "USER_ATTRIBUTE ( 'department' )"})
    void deferredPrincipalCannotEvaluateMaskWithMissingAuthenticationContext(String contextFunction) {
        var subject = DeferredSubjectContext.fromPrincipal("alice", "tenant-one");
        var mask = new ColumnMask(90, "deferred_context", "lake", "default", "users", "email",
                "CASE WHEN " + contextFunction + " = '' THEN {column} ELSE NULL END",
                List.of("reader"), List.of(), true, null);
        var processor = new ColumnMaskingProcessor(new InMemoryColumnMaskStore(List.of(mask)));

        assertThatThrownBy(() -> processor.getMask(subject, Set.of("reader"), "lake", "default", "users", "email"))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("identity-context");
    }

    @Test
    void deferredSubjectGuardDoesNotEvaluateUnselectedOrExemptMasks() {
        var subject = DeferredSubjectContext.fromPrincipal("alice", "tenant-one");
        var mask = new ColumnMask(90, "deferred_context", "lake", "default", "users", "email",
                "user_attribute('department')", List.of("reader"), List.of("unmasked"), true, null);
        var processor = new ColumnMaskingProcessor(new InMemoryColumnMaskStore(List.of(mask)));

        assertThat(processor.getMask(subject, Set.of("reader", "unmasked"),
                "lake", "default", "users", "email")).isEmpty();
        assertThat(processor.getMask(subject, Set.of(), "lake", "default", "users", "email")).isEmpty();
    }

    @Test
    void returnsEmptyWhenNoMasks() {
        ColumnMaskingProcessor processor = new ColumnMaskingProcessor(new EmptyColumnMaskStore());
        Optional<String> mask = processor.getMask(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "users", "email");
        assertThat(mask).isEmpty();
    }

    @Test
    void appliesMaskExpressionWithColumnSubstitution() {
        ColumnMask mask = new ColumnMask(1, "email_mask", "iceberg", "default", "users", "email",
                "MASK_EMAIL({column})", List.of(), List.of(), true, null);
        InMemoryColumnMaskStore store = new InMemoryColumnMaskStore(List.of(mask));
        ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

        Optional<String> result = processor.getMask(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "users", "email");
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("MASK_EMAIL(\"email\")");
    }

    @Test
    void exemptUserSeesRawData() {
        ColumnMask mask = new ColumnMask(1, "email_mask", "iceberg", "default", "users", "email",
                "MASK_EMAIL({column})", List.of(), List.of("admin"), true, null);
        InMemoryColumnMaskStore store = new InMemoryColumnMaskStore(List.of(mask));
        ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

        Optional<String> result = processor.getMask(ADMIN, java.util.Set.of("admin"), "iceberg", "default", "users", "email");
        assertThat(result).isEmpty();
    }

    @Test
    void skipsDisabledMasks() {
        ColumnMask mask = new ColumnMask(1, "disabled_mask", "iceberg", "default", "users", "email",
                "NULL", List.of(), List.of(), false, null);
        InMemoryColumnMaskStore store = new InMemoryColumnMaskStore(List.of(mask));
        ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

        Optional<String> result = processor.getMask(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "users", "email");
        assertThat(result).isEmpty();
    }

    @Test
    void appliesOnlyToMatchingRoles() {
        ColumnMask mask = new ColumnMask(1, "analyst_mask", "iceberg", "default", "users", "email",
                "MASK({column})", List.of("analyst"), List.of(), true, null);
        InMemoryColumnMaskStore store = new InMemoryColumnMaskStore(List.of(mask));
        ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

        assertThat(processor.getMask(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "users", "email")).isPresent();
        assertThat(processor.getMask(ADMIN, java.util.Set.of("admin"), "iceberg", "default", "users", "email")).isEmpty();
    }

    /**
     * Simple in-memory store for testing.
     */
    private static class InMemoryColumnMaskStore implements EnrichedColumnMaskStore {
        private final List<ColumnMask> masks;

        InMemoryColumnMaskStore(List<ColumnMask> masks) {
            this.masks = List.copyOf(masks);
        }

        @Override
        public List<ColumnMask> getTableMasks(String catalog, String schema, String table) {
            return masks.stream().filter(mask -> mask.catalogName().equals(catalog)
                    && mask.schemaName().equals(schema) && mask.tableName().equals(table)).toList();
        }

        @Override
        public List<ColumnMask> getColumnMasks(String catalog, String schema, String table, String column) {
            return masks.stream()
                    .filter(m -> m.catalogName().equals(catalog)
                            && m.schemaName().equals(schema)
                            && m.tableName().equals(table)
                            && m.columnName().equals(column))
                    .toList();
        }

        @Override
        public void saveColumnMask(ColumnMask mask) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteMask(long maskId) {
            throw new UnsupportedOperationException();
        }
    }
}
