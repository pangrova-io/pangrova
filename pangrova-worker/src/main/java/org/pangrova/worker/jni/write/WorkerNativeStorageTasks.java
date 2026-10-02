package org.pangrova.worker.jni.write;

import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.protocol.DataSplitProtoCodec;
import java.util.LinkedHashMap;
import java.util.Optional;

/** Copies locally resolved catalog credentials only into the immediate Worker -> JNI payload. */
public final class WorkerNativeStorageTasks {
    private WorkerNativeStorageTasks() {}

    /** Authenticates every Native source/output deployment before enriching its local JNI payload. */
    public static TaskRequest prepare(TaskRequest request, WorkerCatalogRuntime catalogs) {
        if (request.getWorkerLocalStorageBindingsCount() != 0) {
            throw new SecurityException("Network TaskRequest must not contain Worker-local storage settings");
        }
        var bindings = bindings(request);
        if (bindings.isEmpty()) return request;
        if (catalogs == null) throw new IllegalStateException("Native dataset task requires Worker catalog runtime");
        var local = request.toBuilder();
        for (var entry : bindings.entrySet()) {
            try (var lease = catalogs.leaseControl(entry.getKey(), request.getSecurity(), entry.getValue())) {
                requireLocations(request, entry.getKey(), lease);
                local.addWorkerLocalStorageBindings(lease.storageBinding());
            }
        }
        return local.build();
    }

    private static LinkedHashMap<CatalogExecutionBinding, Optional<WriteOutputBinding>> bindings(TaskRequest request) {
        var output = request.getExecutionOptions().getWrite().getOutputBinding();
        var bindings = new LinkedHashMap<CatalogExecutionBinding, Optional<WriteOutputBinding>>();
        if (output.hasLance()) bindings.put(output.getLance().getCatalog(), Optional.of(output));
        if (output.hasPhysical()) bindings.put(output.getPhysical().getCatalog(), Optional.of(output));
        for (var split : request.getSplitsList()) {
            if (split.hasCatalog()) bindings.putIfAbsent(split.getCatalog(), Optional.empty());
        }
        return bindings;
    }

    private static void requireLocations(TaskRequest request, CatalogExecutionBinding catalog,
            CatalogControlLease lease) {
        var output = request.getExecutionOptions().getWrite().getOutputBinding();
        if (output.hasLance() && output.getLance().getCatalog().equals(catalog)) {
            lease.requireDatasetLocation(output.getLance().getDatasetUri());
        }
        if (output.hasPhysical() && output.getPhysical().getCatalog().equals(catalog)) {
            lease.requireDatasetLocation(output.getPhysical().getAllocationPrefix());
            lease.requireDatasetLocation(output.getPhysical().getAttemptPrefix());
            output.getPhysical().getFilesList().forEach(file -> lease.requireDatasetLocation(file.getOpaqueLocation()));
        }
        for (var split : request.getSplitsList()) {
            if (split.getCatalog().equals(catalog)) {
                lease.requireDatasetLocation(split.getOpaqueLocation());
                requirePhysicalFiles(split, lease);
            }
        }
    }

    private static void requirePhysicalFiles(org.pangrova.protocol.v2.DataSplit split, CatalogControlLease lease) {
        if (!split.getConnectorDetail().is(org.pangrova.protocol.v2.FileSplitDetail.class)) return;
        var detail = DataSplitProtoCodec.fileDetail(split);
        for (var level : detail.getPaimon().getLsmLevelsList()) {
            for (var file : level.getFilesList()) {
                lease.requireDatasetLocation(file.getOpaqueLocation());
                if (file.getScan().hasDeletionVector()) {
                    lease.requireDatasetLocation(file.getScan().getDeletionVector().getOpaqueLocation());
                }
            }
        }
    }
}
