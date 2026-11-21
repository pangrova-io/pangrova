package org.pangrova.connector.api.clustering;

import org.pangrova.connector.api.enums.ClusteringMode;

import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;

/** Clustering maintenance bound to one verified physical table. */
public interface ClusteringMaintenance {

    void setOrder(List<String> columns, ClusteringMode mode, Runnable requireAuthority);

    Status status();

    /** Exact connector revision that admission persists before the first write. */
    String revision();

    /** Returns publication proof or an empty snapshot for a confirmed no-op. */
    OptionalLong rewrite(UUID runId, String expectedRevision, Runnable requireAuthority);

    /** Confirms publication or fences an uncommitted rewrite at the exact revision. */
    Recovery reconcile(UUID runId, String expectedRevision, Runnable requireAuthority);

    record Recovery(OptionalLong snapshot, boolean aborted) {
        public Recovery {
            if (snapshot.isPresent() && aborted) {
                throw new IllegalArgumentException("A published rewrite cannot be aborted");
            }
        }
    }

    record Status(List<String> columns, ClusteringMode mode, int totalFiles, int clusteredFiles) {
        public Status {
            columns = List.copyOf(columns);
        }
    }
}
