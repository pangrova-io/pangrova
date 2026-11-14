package org.pangrova.coordinator.query.execution;

import org.pangrova.query.api.task.QuerySplit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.junit.jupiter.api.Test;

/**
 * Tests for file path extraction from ConnectorSplit via QuerySplit
 * and SplitAssignment construction with file paths for cache affinity routing.
 */
class SplitFilePathExtractionTest {

    @Test
    void splitWithPathsPreservesFilePaths() {
        var swp = new QuerySplit("data", List.of("s3://bucket/file1.parquet"));
        assertThat(swp.serializedData()).isEqualTo("data");
        assertThat(swp.filePaths()).containsExactly("s3://bucket/file1.parquet");
    }

    @Test
    void splitWithPathsDefensiveCopy() {
        var paths = new java.util.ArrayList<>(List.of("s3://a", "s3://b"));
        var swp = new QuerySplit("data", paths);
        paths.add("s3://c");
        assertThat(swp.filePaths()).hasSize(2);
    }

    @Test
    void splitWithPathsNullFilePathsDefaultsToEmpty() {
        var swp = new QuerySplit("data", null);
        assertThat(swp.filePaths()).isEmpty();
    }

    @Test
    void splitAssignmentWithFilePathsForCacheAffinity() {
        long now = System.currentTimeMillis();
        SplitAssignment assignment = new SplitAssignment(
                0, "serialized-data", List.of("node-1"),
                List.of("s3://bucket/table/part-0.parquet"), now);

        assertThat(assignment.splitIndex()).isZero();
        assertThat(assignment.splitData()).isEqualTo("serialized-data");
        assertThat(assignment.dataLocations()).containsExactly("node-1");
        assertThat(assignment.filePaths()).containsExactly("s3://bucket/table/part-0.parquet");
        assertThat(assignment.enqueueTimeMs()).isEqualTo(now);
    }

    @Test
    void splitAssignmentWithMultipleFilePaths() {
        // Paimon splits can have multiple file paths (LSM levels)
        long now = System.currentTimeMillis();
        SplitAssignment assignment = new SplitAssignment(
                1, "paimon-data", List.of(),
                List.of("s3://bucket/level-0/f1.parquet", "s3://bucket/level-1/f2.parquet"), now);

        assertThat(assignment.filePaths()).hasSize(2);
        assertThat(assignment.filePaths()).containsExactly(
                "s3://bucket/level-0/f1.parquet", "s3://bucket/level-1/f2.parquet");
    }

    @Test
    void splitAssignmentConvenienceConstructorHasEmptyFilePaths() {
        SplitAssignment assignment = new SplitAssignment(0, "data", List.of("node-1"));
        assertThat(assignment.filePaths()).isEmpty();
        assertThat(assignment.enqueueTimeMs()).isGreaterThan(0);
    }

    @Test
    void pullModeDispatchBuildsAssignmentsWithFilePaths() {
        // Simulate what dispatchPullMode does: convert QuerySplit to SplitAssignment
        List<QuerySplit> splitsWithPaths = List.of(
                new QuerySplit("s0", List.of("s3://f0.parquet")),
                new QuerySplit("s1", List.of("s3://f1.parquet")),
                new QuerySplit("s2", List.of()));

        long now = System.currentTimeMillis();
        List<SplitAssignment> assignments = new java.util.ArrayList<>();
        for (int i = 0; i < splitsWithPaths.size(); i++) {
            QuerySplit swp = splitsWithPaths.get(i);
            assignments.add(new SplitAssignment(i, swp.serializedData(), List.of(),
                    swp.filePaths(), now));
        }

        assertThat(assignments).hasSize(3);
        assertThat(assignments.get(0).filePaths()).containsExactly("s3://f0.parquet");
        assertThat(assignments.get(1).filePaths()).containsExactly("s3://f1.parquet");
        assertThat(assignments.get(2).filePaths()).isEmpty();
    }
}
