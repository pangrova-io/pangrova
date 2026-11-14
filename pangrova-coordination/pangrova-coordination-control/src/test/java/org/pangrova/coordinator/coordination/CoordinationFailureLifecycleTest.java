package org.pangrova.coordinator.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.persistence.coordination.api.exception.CoordinationStorageException;
import org.pangrova.persistence.coordination.memory.membership.InMemoryCoordinatorRegistry;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import java.lang.reflect.*;
import java.time.Duration;
import java.util.concurrent.atomic.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class CoordinationFailureLifecycleTest {
    static final CoordinationStorageException FAILURE = new CoordinationStorageException("injected backend failure", new IllegalStateException());
    @Test void registrationFailureRollsBackStartupAndCanBeRetried() {
        var failing = new AtomicBoolean(true);
        var registry = new InMemoryCoordinatorRegistry();
        var backend = new InMemoryDistributedLease();
        var service = new HighAvailabilityCoordinationService(backend, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        service.setCoordinatorRegistry(proxy(CoordinatorRegistry.class, registry, (name, call) -> {
            if (name.equals("register") && failing.get()) throw FAILURE;
            return call.call();
        }), "host", 8080);
        assertThatThrownBy(() -> service.start("node")).isSameAs(FAILURE);
        assertThat(service.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STOPPED);
        assertThat(registry.getCoordinator("node")).isEmpty();
        failing.set(false);
        try { service.start("node"); assertThat(service.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER); }
        finally { service.close(); }
    }
    @Test void unknownAcquisitionCannotPublishLeaderOrLeakRunningStartup() {
        var backend = new InMemoryDistributedLease();
        var service = new HighAvailabilityCoordinationService(proxy(DistributedLease.class, backend, (name, call) -> {
            Object result = call.call();
            if (name.equals("tryAcquire")) throw FAILURE;
            return result;
        }), new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        assertThatThrownBy(() -> service.start("node")).isSameAs(FAILURE);
        assertThat(service.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STOPPED);
        assertThat(service.hasActiveLeader()).as("unknown grant may really exist; failure is not absence").isTrue();
        service.close();
    }
    @Test void stepDownSealsLocalRoleBeforeUnknownRelease() {
        var backend = new InMemoryDistributedLease();
        var service = new HighAvailabilityCoordinationService(proxy(DistributedLease.class, backend, (name, call) -> {
            if (name.equals("release")) throw FAILURE;
            return call.call();
        }), new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        var demoted = new AtomicInteger();
        service.onDemotion(demoted::incrementAndGet);
        service.tryBecomeLeader("node");
        assertThatThrownBy(() -> service.stepDown("node")).isSameAs(FAILURE);
        assertThat(service.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STANDBY);
        assertThat(demoted).hasValue(1);
        service.close();
    }
    @Test void closeAttemptsUnregisterAndStopsLocallyEvenWhenReleaseFails() {
        var backend = new InMemoryDistributedLease();
        var unregister = new AtomicInteger();
        var service = new HighAvailabilityCoordinationService(proxy(DistributedLease.class, backend, (name, call) -> {
            if (name.equals("release")) throw new IllegalStateException("release failed");
            return call.call();
        }), new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        service.setCoordinatorRegistry(proxy(CoordinatorRegistry.class, new InMemoryCoordinatorRegistry(), (name, call) -> {
            if (name.equals("unregister")) { unregister.incrementAndGet(); throw new IllegalStateException("unregister failed"); }
            return call.call();
        }), "host", 8080);
        service.start("node");
        assertThatThrownBy(service::close).hasMessage("release failed")
                .satisfies(f -> assertThat(f.getSuppressed()).singleElement().satisfies(t -> assertThat(t).hasMessage("unregister failed")));
        assertThat(unregister).hasValue(1);
        assertThat(service.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STOPPED);
    }
    @Test void renewalAndRolePublicationFailureStillStopLeaderLoops() {
        var fail = new AtomicBoolean();
        var demoted = new AtomicInteger();
        var service = new HighAvailabilityCoordinationService(proxy(DistributedLease.class, new InMemoryDistributedLease(), (name, call) -> {
            if (name.equals("tryRenew") && fail.get()) throw FAILURE;
            return call.call();
        }), new HighAvailabilityCoordinationService.CoordinationConfig(1000, 20), new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        service.setCoordinatorRegistry(proxy(CoordinatorRegistry.class, new InMemoryCoordinatorRegistry(), (name, call) -> {
            if (name.equals("updateRole") && fail.get()) throw FAILURE;
            return call.call();
        }), "host", 8080);
        service.onDemotion(demoted::incrementAndGet);
        try {
            service.start("node"); fail.set(true);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(service.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STANDBY);
                assertThat(demoted.get()).isPositive();
            });
        } finally { fail.set(false); service.close(); }
    }
    @Test void failedLeaderActivationReleasesGrantAndRollsBackStartup() {
        var lease = new InMemoryDistributedLease();
        var service = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        var demotions = new AtomicInteger();
        service.onFailover(id -> { throw new IllegalStateException("activation failed"); });
        service.onDemotion(demotions::incrementAndGet);
        assertThatThrownBy(() -> service.start("node")).hasMessage("activation failed");
        assertThat(service.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STOPPED);
        assertThat(service.hasActiveLeader()).isFalse();
        assertThat(demotions.get()).isPositive();
        service.close();
    }
    @Test void demotionCallbackFailureDoesNotSkipRemoteRelease() {
        var service = new HighAvailabilityCoordinationService(new InMemoryDistributedLease(), new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        service.tryBecomeLeader("node");
        service.onDemotion(() -> { throw new IllegalStateException("demotion failed"); });
        assertThatThrownBy(() -> service.stepDown("node")).hasMessage("demotion failed");
        assertThat(service.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STANDBY);
        assertThat(service.hasActiveLeader()).isFalse();
        service.onDemotion(() -> {});
        service.close();
    }
    @Test void failedDemotionCallbackStillPublishesStandbyRegistryRole() {
        var failed = new AtomicBoolean();
        var registry = new InMemoryCoordinatorRegistry();
        var service = new HighAvailabilityCoordinationService(proxy(DistributedLease.class, new InMemoryDistributedLease(), (name, call) -> {
            if (name.equals("tryRenew") && failed.get()) throw FAILURE;
            return call.call();
        }), new HighAvailabilityCoordinationService.CoordinationConfig(1000, 20), new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        service.setCoordinatorRegistry(registry, "host", 8080);
        service.onDemotion(() -> { throw new IllegalStateException("partial demotion failed"); });
        try {
            service.start("node"); failed.set(true);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(service.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STANDBY);
                assertThat(registry.getCoordinator("node").orElseThrow().role()).isEqualTo("STANDBY");
            });
        } finally {
            failed.set(false); service.onDemotion(() -> {}); service.close();
        }
    }
    interface Invocation { Object call() throws Throwable; }
    interface Fault { Object apply(String name, Invocation invocation) throws Throwable; }
    static <T> T proxy(Class<T> type, T target, Fault fault) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (p, method, args) ->
                fault.apply(method.getName(), () -> {
                    try { return method.invoke(target, args); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                })));
    }
}
