package org.pangrova.metadata.file;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.storage.file.FileListing;
import org.pangrova.spi.storage.file.FileSpace;
import org.pangrova.spi.storage.file.FileSpaceDefinition;
import org.pangrova.spi.storage.file.FileSpaceMutation;
import org.pangrova.spi.storage.file.FileSpaceType;
import org.pangrova.spi.storage.file.FileStat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcFileDirectoryIndexTest {
    private JdbcFileSpaceRegistry spaces;
    private JdbcFileDirectoryIndex index;
    private FileSpace stage;

    @BeforeEach
    void setUp() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("directory_index");
        spaces = new JdbcFileSpaceRegistry(source);
        index = new JdbcFileDirectoryIndex(source);
        stage = spaces.create(new FileSpaceDefinition(FileSpaceType.STAGE, "media", "raw", "landing",
                false, "local", "", true, "", "alice"), false);
    }

    @Test
    void incompleteAndAbandonedScansDoNotReplacePublishedFacts() {
        publish("old.txt");
        var token = index.begin(stage, "");
        index.append(token, null, new FileListing(List.of(file("new.txt")), "next"));
        assertPaths("old.txt");
        assertThatThrownBy(() -> index.publish(token)).hasMessageContaining("incomplete");
        assertPaths("old.txt");
        index.abort(token);
        assertPaths("old.txt");
    }

    @Test
    void emptyIntermediatePageIsNotProofOfDeletionAndTerminalPagePublishesExactMetadata() {
        publish("old.txt");
        var token = index.begin(stage, "");
        index.append(token, null, new FileListing(List.of(), "next"));
        assertPaths("old.txt");
        index.append(token, "next", new FileListing(List.of(file("new.txt")), null));
        assertPaths("old.txt");
        assertThat(index.publish(token)).isEqualTo(1);
        assertThat(index.page(stage, null, 10)).containsExactly(file("new.txt"));
        assertThatThrownBy(() -> index.publish(token)).hasMessageContaining("finished");
    }

    @Test
    void subpathRefreshPreservesAdjacentDirectoriesAndTreatsSqlWildcardsLiterally() {
        publish("a_/old.txt", "aX/keep.txt", "a_extra/keep.txt", "root.txt");
        var token = index.begin(stage, "a_/");
        index.append(token, null, new FileListing(List.of(), null));
        index.publish(token);
        assertPaths("aX/keep.txt", "a_extra/keep.txt", "root.txt");
    }

    @Test
    void subpathRefreshReplacesOnlyItsDirectoryBoundary() {
        publish("images/old.txt", "images-extra/keep.txt", "root.txt");
        var token = index.begin(stage, "images/");
        index.append(token, null, new FileListing(List.of(file("images/new.txt")), null));
        index.publish(token);
        assertPaths("images-extra/keep.txt", "images/new.txt", "root.txt");
        token = index.begin(stage, "images");
        index.append(token, null, new FileListing(List.of(), null));
        index.publish(token);
        assertPaths("images-extra/keep.txt", "root.txt");
    }

    @Test
    void staleAttemptCannotAppendPublishOrAbortItsReplacement() {
        var first = index.begin(stage, "");
        index.append(first, null, new FileListing(List.of(file("first.txt")), null));
        var second = index.begin(stage, "");
        assertThatThrownBy(() -> index.append(first, null, new FileListing(List.of(), null)))
                .hasMessageContaining("superseded");
        assertThatThrownBy(() -> index.publish(first)).hasMessageContaining("superseded");
        index.abort(first);
        index.append(second, null, new FileListing(List.of(file("second.txt")), null));
        index.publish(second);
        assertPaths("second.txt");
    }

    @Test
    void wrongCheckpointAndOutOfScopePageAreRolledBack() {
        var token = index.begin(stage, "images");
        assertThatThrownBy(() -> index.append(token, "stale", new FileListing(List.of(), null)))
                .hasMessageContaining("stale");
        assertThatThrownBy(() -> index.append(token, null, new FileListing(
                List.of(file("images/good.txt"), file("other.txt")), null)))
                .hasMessageContaining("outside");
        index.append(token, null, new FileListing(List.of(), null));
        index.publish(token);
        assertPaths();
    }

    @Test
    void locationChangesInvalidateBothPublishedAndUnfinishedObservations() {
        publish("old.txt");
        var token = index.begin(stage, "");
        index.append(token, null, new FileListing(List.of(file("unpublished.txt")), null));
        stage = spaces.mutate(FileSpaceType.STAGE, "media", "raw", "landing",
                new FileSpaceMutation.SetStorageProfile("other"));
        assertPaths();
        assertThatThrownBy(() -> index.publish(token)).isInstanceOf(MetastoreException.class);
    }

    @Test
    void renamePreservesPublishedMetadataButDropAndRecreateDoNot() {
        publish("old.txt");
        stage = spaces.rename(FileSpaceType.STAGE, "media", "raw", "landing", "renamed");
        assertPaths("old.txt");
        spaces.drop(FileSpaceType.STAGE, "media", "raw", "renamed", false);
        stage = spaces.create(new FileSpaceDefinition(FileSpaceType.STAGE, "media", "raw", "renamed",
                false, "local", "", true, "", "alice"), false);
        assertPaths();
    }

    private void publish(String... paths) {
        var token = index.begin(stage, "");
        index.append(token, null, new FileListing(java.util.Arrays.stream(paths)
                .map(JdbcFileDirectoryIndexTest::file).toList(), null));
        index.publish(token);
    }

    private void assertPaths(String... paths) {
        assertThat(index.page(stage, null, 100)).extracting(FileStat::path).containsExactly(paths);
    }

    private static FileStat file(String path) {
        return new FileStat(path, 123, Instant.parse("2026-10-25T00:00:00Z"),
                "provider-etag", "application/octet-stream", "version-1", null);
    }
}
