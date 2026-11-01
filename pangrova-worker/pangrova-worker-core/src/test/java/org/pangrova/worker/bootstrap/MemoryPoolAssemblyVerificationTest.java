package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

import org.pangrova.testing.nativefixture.TestShuffleServiceConfig;
import org.pangrova.worker.resource.memory.RootMemoryPool;
import org.pangrova.worker.resource.memory.NodeMemoryPool;
import org.pangrova.worker.resource.memory.QueryMemoryPool;
import org.pangrova.worker.resource.memory.TaskMemoryPool;
import org.pangrova.worker.nativebridge.jni.write.TestCatalogResourceResolver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the worker resource memory pool hierarchy is correctly assembled
 * during WorkerBootstrap and injected into TaskManager.
 *
 * <p>Traces the full chain:
 * WorkerResourceBootstrap → RootMemoryPool → NodeMemoryPool →
 * TaskManagerContext → TaskManager → TaskMemoryAllocator →
 * (per-task) QueryMemoryPool → TaskMemoryPool → NativeRuntime.nativeCommand (UPDATE_MEMORY_LIMIT)</p>
 *
 * <p><b>Validates: Requirements 62.1, 62.2</b></p>
 */
@Tag("native-required")
class MemoryPoolAssemblyVerificationTest {

    private static StubCoordinatorServer stubCoordinator;
    private WorkerBootstrap bootstrap;

    @BeforeAll
    static void startStub() throws Exception {
        stubCoordinator = StubCoordinatorServer.start();
    }

    @AfterAll
    static void stopStub() {
        if (stubCoordinator != null) {
            stubCoordinator.close();
        }
    }

    @AfterEach
    void tearDown() {
        if (bootstrap != null && bootstrap.isStarted()) {
            bootstrap.shutdown();
        }
    }

    @Test
    void nodeMemoryPoolIsInitializedDuringBootstrap() {
        startWorker("mem-pool-init");

        NodeMemoryPool nodePool = bootstrap.getNodeMemoryPool();
        assertThat(nodePool).isNotNull();
        assertThat(nodePool.getMaxBytes()).isGreaterThan(0);
        assertThat(nodePool.getName()).contains("node-");
    }

    @Test
    void rootMemoryPoolIsParentOfNodePool() throws Exception {
        startWorker("mem-pool-hierarchy");

        RootMemoryPool rootPool = getRootPool();
        NodeMemoryPool nodePool = bootstrap.getNodeMemoryPool();

        assertThat(rootPool).isNotNull();
        assertThat(nodePool).isNotNull();
        assertThat(nodePool.getParent()).isSameAs(rootPool);
    }

    @Test
    void nodeMemoryPoolIsInjectedIntoTaskManager() throws Exception {
        startWorker("mem-pool-inject");

        NodeMemoryPool nodePool = bootstrap.getNodeMemoryPool();
        assertThat(nodePool).isNotNull();

        // TaskManager delegates lifecycle to TaskLifecycleManager which holds the allocator
        Object lifecycleManager = getTaskManagerField("lifecycleManager");
        assertThat(lifecycleManager).isNotNull();

        Field allocatorField = lifecycleManager.getClass().getDeclaredField("memoryAllocator");
        allocatorField.setAccessible(true);
        Object allocator = allocatorField.get(lifecycleManager);
        assertThat(allocator).isNotNull();

        Field nodePoolField = allocator.getClass().getDeclaredField("nodePool");
        nodePoolField.setAccessible(true);
        NodeMemoryPool injectedPool = (NodeMemoryPool) nodePoolField.get(allocator);
        assertThat(injectedPool).isSameAs(nodePool);
    }

    @Test
    void nodePoolReservesFromClusterPool() throws Exception {
        startWorker("mem-pool-reserve");

        RootMemoryPool rootPool = getRootPool();
        NodeMemoryPool nodePool = bootstrap.getNodeMemoryPool();

        // NodeMemoryPool was created with nodeMemoryBytes from config,
        // which should have reserved that amount from RootMemoryPool
        assertThat(rootPool.getUsedBytes()).isEqualTo(nodePool.getMaxBytes());
    }

    @Test
    void queryAndTaskPoolsCanBeCreatedFromNodePool() {
        startWorker("mem-pool-child");

        NodeMemoryPool nodePool = bootstrap.getNodeMemoryPool();
        long queryBytes = 64L * 1024 * 1024;

        QueryMemoryPool queryPool = nodePool.createQueryPool("test-query", queryBytes);
        assertThat(queryPool).isNotNull();
        assertThat(queryPool.getMaxBytes()).isEqualTo(queryBytes);
        assertThat(queryPool.getParent()).isSameAs(nodePool);

        long taskBytes = 32L * 1024 * 1024;
        TaskMemoryPool taskPool = queryPool.createTaskPool("test-task", taskBytes);
        assertThat(taskPool).isNotNull();
        assertThat(taskPool.getMaxBytes()).isEqualTo(taskBytes);
        assertThat(taskPool.getParent()).isSameAs(queryPool);

        // Clean up: release in reverse order
        queryPool.free(taskBytes);
        nodePool.free(queryBytes);
    }

    @Test
    void memoryPoolConfigMatchesBootstrapConfig() {
        long customNodeMemory = 256L * 1024 * 1024;
        WorkerBootstrapConfig.ResourceConfig rc = new WorkerBootstrapConfig.ResourceConfig(
                512L * 1024 * 1024, 64L * 1024 * 1024, 4, 2, 1, null, customNodeMemory,
                50.0, 70.0, 85.0, 95.0,
                WorkerBootstrapConfig.ResourceConfig.DEFAULT_SHUFFLE_RETENTION,
                WorkerBootstrapConfig.ResourceConfig.DEFAULT_SHUFFLE_TRANSIT,
                WorkerBootstrapConfig.ResourceConfig.DEFAULT_SHUFFLE_SPILL,
                WorkerBootstrapConfig.ResourceConfig.DEFAULT_SHUFFLE_SPILL_FILES, null,
                WorkerBootstrapConfig.ResourceConfig.defaults().shuffleChannelLimits());
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "mem-pool-config", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, rc, 0, false, new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        bootstrap = configuredBootstrap(config);
        bootstrap.start();

        NodeMemoryPool nodePool = bootstrap.getNodeMemoryPool();
        assertThat(nodePool.getMaxBytes()).isEqualTo(customNodeMemory);
    }

    // -- Helpers --

    private void startWorker(String workerId) {
        bootstrap = configuredBootstrap(configWithDefaults(workerId));
        bootstrap.start();
    }

    private static WorkerBootstrap configuredBootstrap(WorkerBootstrapConfig config) {
        WorkerBootstrap worker = new WorkerBootstrap(TestShuffleServiceConfig.config(config));
        worker.setCatalogResourceResolver(new TestCatalogResourceResolver((taskId) -> {
            throw new AssertionError("Memory pool assembly must not resolve a catalog");
        }));
        return worker;
    }

    private static WorkerBootstrapConfig configWithDefaults(String workerId) {
        return new WorkerBootstrapConfig(
                workerId, 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, WorkerBootstrapConfig.ResourceConfig.defaults(),
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
    }

    private RootMemoryPool getRootPool() throws Exception {
        Object refs = getField(bootstrap, "refs");
        Field field = refs.getClass().getDeclaredField("rootMemoryPool");
        field.setAccessible(true);
        return (RootMemoryPool) field.get(refs);
    }

    private Object getTaskManagerField(String fieldName) throws Exception {
        Object taskManager = bootstrap.getTaskManager();
        Field field = taskManager.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(taskManager);
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
