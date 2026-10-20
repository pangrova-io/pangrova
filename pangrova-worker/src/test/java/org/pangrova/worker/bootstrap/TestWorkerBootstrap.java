package org.pangrova.worker.bootstrap;

import org.pangrova.testing.nativefixture.TestShuffleServiceConfig;

import org.pangrova.worker.jni.write.TestCatalogResourceResolver;

/** Native bootstrap lifecycle fixture; it must never accept actual catalog work. */
public final class TestWorkerBootstrap {
    private TestWorkerBootstrap() {}

    public static WorkerBootstrap create(WorkerBootstrapConfig config) {
        WorkerBootstrap worker = new WorkerBootstrap(TestShuffleServiceConfig.config(config));
        worker.setCatalogResourceResolver(new TestCatalogResourceResolver((taskId) -> {
            throw new AssertionError("bootstrap lifecycle fixture must not resolve catalog tasks");
        }));
        return worker;
    }
}
