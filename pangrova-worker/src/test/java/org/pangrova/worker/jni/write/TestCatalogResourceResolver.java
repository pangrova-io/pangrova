package org.pangrova.worker.jni.write;

import org.pangrova.worker.jni.write.catalog.CatalogResourceResolver;

import org.pangrova.protocol.v2.TaskRequest;

import java.util.function.Function;

/** Explicitly task-scoped resolver used only by Worker unit tests. */
public final class TestCatalogResourceResolver implements CatalogResourceResolver {

    private final Function<String, CatalogResources> resolver;

    public TestCatalogResourceResolver(
            Function<String, CatalogResources> resolver) {
        this.resolver = resolver;
    }

    @Override
    public void bindTask(String taskId, TaskRequest request) {
        // Unit tests invoke the JNI adapter directly without a TaskManager.
    }

    @Override
    public CatalogResources resolve(String taskId) {
        return resolver.apply(taskId);
    }

    @Override
    public void releaseTask(String taskId) {
        // No shared connector lease in unit tests.
    }
}
