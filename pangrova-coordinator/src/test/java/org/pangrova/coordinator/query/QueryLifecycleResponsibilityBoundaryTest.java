package org.pangrova.coordinator.query;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies responsibility boundaries between Coordinator query lifecycle components.
 * Ensures each component maintains a single, clear responsibility without overlap.
 *
 * <ul>
 *   <li>{@link QueryManager}: query lifecycle state machines, audit, lineage, tenant</li>
 *   <li>{@link QueryDispatcher}: compile → schedule → dispatch orchestration</li>
 *   <li>{@link TaskStatusReceiver}: task status callback processing</li>
 *   <li>{@link QueryTimeoutEnforcer}: periodic timeout enforcement</li>
 * </ul>
 *
 * <p>Plan fragmentation has been moved to {@code pangrova-planner.PlanFragmenter};
 * the historical {@code StageFragmenter} class no longer exists in this module,
 * so the related boundary checks are unconditionally satisfied and have been
 * removed from this suite.</p>
 *
 * Validates: Requirements 3.2
 */
class QueryLifecycleResponsibilityBoundaryTest {

    // ── QueryManager boundary: lifecycle orchestration only ──

    @Test
    void queryManager_doesNotDirectlyReferenceStatementDispatcher() {
        assertClassDoesNotReference(QueryManager.class, StatementDispatcher.class,
                "QueryManager must not directly depend on StatementDispatcher — "
                        + "DDL/DML routing is QueryDispatcher's responsibility");
    }

    @Test
    void queryManager_publicApiIsLifecycleOnly() {
        Set<String> publicMethods = publicMethodNames(QueryManager.class);
        // QueryManager public API should be lifecycle-oriented
        assertThat(publicMethods).as("QueryManager public API")
                .contains("submitQuery", "cancelQuery", "getQueryState",
                        "getResultEndpoint", "getActiveQueryCount",
                        "onAllTasksFinished", "onTaskFailed");
        // Must NOT contain dispatch/compile/fragment methods
        assertThat(publicMethods).as("QueryManager must not expose dispatch methods")
                .doesNotContain("dispatch", "compile", "fragment",
                        "dispatchToWorker", "tryExecuteDdlDml");
    }

    // ── QueryDispatcher boundary: compile → schedule → dispatch ──

    @Test
    void queryDispatcher_doesNotDirectlyManageStateMachineMap() {
        // QueryDispatcher receives QueryStateMachine as parameter, does not own the map
        Set<String> fieldTypes = declaredFieldTypeNames(QueryDispatcher.class);
        assertThat(fieldTypes).as("QueryDispatcher must not own query state machine map")
                .doesNotContain("ConcurrentHashMap");
    }

    @Test
    void queryDispatcher_ddlDmlRoutingDelegatesToDispatcher() {
        // DDL/DML routing now flows QueryDispatcher → QueryPipelineExecutor → StatementDispatcher.
        // The dispatcher holds QueryPipelineExecutor as the pipeline owner; the StatementDispatcher
        // is encapsulated by the pipeline executor instead of being a direct field.
        Set<String> fieldTypes = declaredFieldTypeNames(QueryDispatcher.class);
        assertThat(fieldTypes)
                .as("QueryDispatcher should delegate DDL/DML routing through QueryPipelineExecutor")
                .contains("QueryPipelineExecutor");
    }

    @Test
    void queryDispatcher_doesNotContainDdlClassificationMethods() {
        Set<String> methods = allDeclaredMethodNames(QueryDispatcher.class);
        assertThat(methods).as("QueryDispatcher must not contain DDL classification methods")
                .doesNotContain("isDdlStatement", "isDmlStatement",
                        "isShowStatement", "isMaintenanceStatement");
    }

    // ── TaskStatusReceiver boundary: status callback only ──

    @Test
    void taskStatusReceiver_doesNotReferenceQueryDispatcher() {
        assertClassDoesNotReference(TaskStatusReceiver.class, QueryDispatcher.class,
                "TaskStatusReceiver must not depend on QueryDispatcher — "
                        + "it only drives state transitions via QueryManager");
    }

    @Test
    void taskStatusReceiver_publicApiIsCallbackOnly() {
        Set<String> publicMethods = publicMethodNames(TaskStatusReceiver.class);
        assertThat(publicMethods).as("TaskStatusReceiver public API")
                // setWriteCommitFencer is the A-FENCE-01 late-wiring seam: the leadership
                // fence is built after HA bootstrap, mirroring the QueryManager
                // setClusterAdmissionController pattern. setWriteQualityGate is the same
                // kind of seam for the governance quality gate, which depends on module
                // wiring. Both configure, never drive, the callback flow — so the
                // callback-only boundary still holds.
                .containsExactlyInAnyOrder("onTaskStatus", "clearTaskSequence",
                        "setWriteCommitFencer", "setWriteQualityGate");
    }

    // ── QueryTimeoutEnforcer boundary: timeout enforcement only ──

    @Test
    void queryTimeoutEnforcer_doesNotReferenceQueryDispatcher() {
        assertClassDoesNotReference(QueryTimeoutEnforcer.class, QueryDispatcher.class,
                "QueryTimeoutEnforcer must not depend on QueryDispatcher — "
                        + "it only enforces timeouts on state machines");
    }

    @Test
    void queryTimeoutEnforcer_publicApiIsStartStopOnly() {
        Set<String> publicMethods = publicMethodNames(QueryTimeoutEnforcer.class);
        assertThat(publicMethods).as("QueryTimeoutEnforcer public API")
                .containsExactlyInAnyOrder("start", "stop", "setTimeoutCallback");
    }

    // ── StatementDispatcher boundary: statement dispatch only ──

    @Test
    void statementDispatcher_ownsClassificationLogic() {
        Set<String> methods = allDeclaredMethodNames(
                org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher.class);
        assertThat(methods).as("StatementDispatcher must own statement classification")
                .contains("isRoutable");
    }

    // ── Cross-cutting: constructor dependency governance ──

    @Test
    void queryManager_usesParameterObject() {
        assertConstructorParamCount(QueryManager.class, 1,
                "QueryManager must use QueryManagerContext parameter object");
    }

    @Test
    void queryDispatcher_constructorWithinLimit() {
        // QueryDispatcher has a 3-param public constructor and a 4-param public constructor
        // (deps, fragmenter, preBuiltDispatcher, tracingProvider).
        // The deps parameter object keeps the effective dependency count low.
        assertConstructorParamCount(QueryDispatcher.class, 5,
                "QueryDispatcher constructor must not exceed 5 parameters");
    }

    @Test
    void taskStatusReceiver_constructorWithinLimit() {
        assertConstructorParamCount(TaskStatusReceiver.class, 3,
                "TaskStatusReceiver constructor must not exceed 4 parameters");
    }

    // ── Helpers ──

    private static Set<String> publicMethodNames(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredMethods())
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .filter(m -> !m.isSynthetic())
                .map(Method::getName)
                .collect(Collectors.toSet());
    }

    private static Set<String> allDeclaredMethodNames(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredMethods())
                .filter(m -> !m.isSynthetic())
                .map(Method::getName)
                .collect(Collectors.toSet());
    }

    private static Set<String> declaredFieldTypeNames(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .map(f -> f.getType().getSimpleName())
                .collect(Collectors.toSet());
    }

    private static void assertClassDoesNotReference(
            Class<?> source, Class<?> forbidden, String message) {
        Set<String> fieldTypes = Arrays.stream(source.getDeclaredFields())
                .map(f -> f.getType().getName())
                .collect(Collectors.toSet());
        assertThat(fieldTypes).as(message)
                .doesNotContain(forbidden.getName());
    }

    private static void assertConstructorParamCount(
            Class<?> clazz, int expectedMax, String message) {
        int maxParams = Arrays.stream(clazz.getDeclaredConstructors())
                .filter(c -> Modifier.isPublic(c.getModifiers()))
                .mapToInt(c -> c.getParameterCount())
                .max()
                .orElse(0);
        assertThat(maxParams).as(message)
                .isLessThanOrEqualTo(expectedMax);
    }
}
