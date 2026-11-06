package org.pangrova.security.policy.builtin.authorization;

import org.junit.jupiter.api.Test;
import org.pangrova.file.api.space.FileSpaceType;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;

import static org.assertj.core.api.Assertions.assertThat;

class FileSpaceGrantMatcherTest {
    @Test
    void volumeAndStageNamesDoNotShareAuthority() {
        var volume = grant("VOLUME", "catalog", "schema", "media", null);
        var stage = grant("STAGE", "catalog", "schema", "media", null);
        assertThat(FileSpaceGrantMatcher.covers(volume, FileSpaceType.VOLUME, "catalog", "schema", "media")).isTrue();
        assertThat(FileSpaceGrantMatcher.covers(volume, FileSpaceType.STAGE, "catalog", "schema", "media")).isFalse();
        assertThat(FileSpaceGrantMatcher.covers(stage, FileSpaceType.STAGE, "catalog", "schema", "media")).isTrue();
        assertThat(FileSpaceGrantMatcher.covers(stage, FileSpaceType.VOLUME, "catalog", "schema", "media")).isFalse();
        assertThat(volume.coversTable("catalog", "schema", "media")).isFalse();
        assertThat(stage.coversColumn("catalog", "schema", "media", "column")).isFalse();
    }

    @Test
    void namespaceScopesCascadeButExactSpaceCannotAuthorizeSchemaScope() {
        var catalog = grant("CATALOG", "catalog", null, null, null);
        var schema = grant("SCHEMA", "catalog", "schema", "*", null);
        var exact = grant("STAGE", "catalog", "schema", "media", null);
        for (var type : FileSpaceType.values()) {
            assertThat(FileSpaceGrantMatcher.covers(catalog, type, "catalog", "schema", "media")).isTrue();
            assertThat(FileSpaceGrantMatcher.covers(schema, type, "catalog", "schema", null)).isTrue();
            assertThat(FileSpaceGrantMatcher.covers(schema, type, "catalog", "other", "media")).isFalse();
        }
        assertThat(FileSpaceGrantMatcher.covers(exact, FileSpaceType.STAGE, "catalog", "schema", null)).isFalse();
        assertThat(FileSpaceGrantMatcher.covers(exact, FileSpaceType.STAGE, "other", "schema", "media")).isFalse();
    }

    @Test
    void tableColumnStorageAndUnknownScopesCannotAuthorizeSpaces() {
        for (String kind : new String[]{"TABLE", "COLUMN", "STORAGE_PROFILE", "UNKNOWN"}) {
            var grant = grant(kind, "catalog", "schema", "media", null);
            for (var type : FileSpaceType.values()) {
                assertThat(FileSpaceGrantMatcher.covers(grant, type, "catalog", "schema", "media"))
                        .as("%s cannot authorize %s", kind, type).isFalse();
            }
        }
        var columnScoped = grant("SCHEMA", "catalog", "schema", "*", "column");
        assertThat(FileSpaceGrantMatcher.covers(columnScoped, FileSpaceType.STAGE, "catalog", "schema", "media")).isFalse();
    }

    @Test
    void caseInsensitiveKindsAndExplicitWildcardsRetainTheirMeaning() {
        var grant = grant("stage", "*", "schema", "*", "*");
        assertThat(FileSpaceGrantMatcher.covers(grant, FileSpaceType.STAGE, "catalog", "schema", "media")).isTrue();
        assertThat(FileSpaceGrantMatcher.covers(grant, FileSpaceType.STAGE, "catalog", "SCHEMA", "media")).isFalse();
        assertThat(FileSpaceGrantMatcher.covers(grant, FileSpaceType.VOLUME, "catalog", "schema", "media")).isFalse();
    }

    private static PrivilegeGrant grant(String kind, String catalog, String schema, String space, String column) {
        return PrivilegeGrant.of("role", Privilege.SELECT, kind, catalog, schema, space, column);
    }
}
