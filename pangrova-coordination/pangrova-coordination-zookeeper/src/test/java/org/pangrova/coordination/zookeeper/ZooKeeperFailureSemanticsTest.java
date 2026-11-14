package org.pangrova.coordination.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordination.api.membership.CoordinatorSnapshot;
import org.pangrova.coordination.api.enums.CoordinatorRole;
import org.pangrova.persistence.coordination.api.exception.CoordinationStorageException;
import java.nio.file.Path;
import java.net.InetSocketAddress;
import java.lang.reflect.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

/** Real ZooKeeper with faults around actual client operations, including lost commit acknowledgements. */
class ZooKeeperFailureSemanticsTest {
    @TempDir Path directory;
    ZooKeeperServer server;
    ServerCnxnFactory connections;
    CuratorFramework client;
    ZooKeeperCoordinatorRegistry registry;
    ZooKeeperDistributedLease lease;
    static final Duration TTL = Duration.ofSeconds(30);
    static CoordinatorSnapshot snapshot() {
        long now = System.currentTimeMillis();
        return new CoordinatorSnapshot("node", "localhost", 8080, "STANDBY", 1, now, now, 0);
    }
    @BeforeEach void start() throws Exception {
        System.setProperty("zookeeper.admin.enableServer", "false");
        server = new ZooKeeperServer(directory.toFile(), directory.toFile(), 2000);
        connections = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", 0), 100);
        connections.startup(server);
        client = CuratorFrameworkFactory.newClient("127.0.0.1:" + connections.getLocalPort(), new RetryOneTime(1));
        client.start();
        assertThat(client.blockUntilConnected(10, TimeUnit.SECONDS)).isTrue();
        registry = new ZooKeeperCoordinatorRegistry(client, "/registry");
        lease = new ZooKeeperDistributedLease(client, "/leases");
    }
    @AfterEach void close() {
        Thread.interrupted();
        if (client != null) client.close();
        if (connections != null) connections.shutdown();
        if (server != null) server.shutdown();
    }
    @Test void closedClientIsNeverAbsenceOrSuccessfulMutation() {
        registry.register(snapshot());
        long epoch = lease.tryAcquire("key", "node", TTL).orElseThrow().epoch();
        client.close();
        List<Runnable> operations = List.of(
                () -> registry.register(snapshot()), () -> registry.heartbeat("node"),
                () -> registry.updateRole("node", CoordinatorRole.LEADER),
                () -> registry.updateLastProcessedInvalidationId("node", 20),
                () -> registry.unregister("node"), () -> registry.getCoordinator("node"),
                () -> registry.getLeader(), () -> registry.getAllCoordinators(TTL),
                () -> registry.expireStale(TTL), () -> lease.tryAcquire("key", "node", TTL),
                () -> lease.tryRenew("key", "node", epoch, TTL),
                () -> lease.release("key", "node", epoch), () -> lease.currentHolder("key"));
        for (Runnable operation : operations) assertThatThrownBy(operation::run)
                .isInstanceOf(CoordinationStorageException.class).hasCauseInstanceOf(IllegalStateException.class);
    }
    @Test void missingClockIsNotMissingLease() throws Exception {
        long epoch = lease.tryAcquire("key", "node", TTL).orElseThrow().epoch();
        client.delete().forPath("/leases/_clock");
        for (Runnable operation : List.<Runnable>of(() -> lease.currentHolder("key"),
                () -> lease.tryRenew("key", "node", epoch, TTL), () -> lease.release("key", "node", epoch))) {
            assertThatThrownBy(operation::run).isInstanceOf(CoordinationStorageException.class)
                    .hasCauseInstanceOf(KeeperException.NoNodeException.class);
        }
    }
    @Test void corruptRecordNeverProducesPartialTopology() throws Exception {
        registry.register(snapshot());
        client.create().forPath("/registry/broken", new byte[]{1, 2});
        assertThatThrownBy(() -> registry.getAllCoordinators(TTL)).isInstanceOf(CoordinationStorageException.class);
        assertThatThrownBy(() -> registry.getCoordinator("broken")).isInstanceOf(CoordinationStorageException.class);
        assertThatThrownBy(() -> registry.expireStale(TTL)).isInstanceOf(CoordinationStorageException.class);
    }
    @Test void committedRegistrationWithLostAcknowledgementIsReportedUnknown() {
        var faulty = new ZooKeeperCoordinatorRegistry(inject((op, path, invocation) -> {
            Object result = invocation.call();
            if (op.equals("create") && path.equals("/registry/node")) throw new KeeperException.ConnectionLossException();
            return result;
        }), "/registry");
        assertThatThrownBy(() -> faulty.register(snapshot())).isInstanceOf(CoordinationStorageException.class)
                .hasCauseInstanceOf(KeeperException.ConnectionLossException.class);
        assertThat(registry.getCoordinator("node")).isPresent();
    }
    @Test void committedLeaseWithLostAcknowledgementIsNotFalseContention() {
        var faulty = new ZooKeeperDistributedLease(inject((op, path, invocation) -> {
            Object result = invocation.call();
            if (op.equals("create") && path.startsWith("/leases/k-")) throw new KeeperException.ConnectionLossException();
            return result;
        }), "/leases");
        assertThatThrownBy(() -> faulty.tryAcquire("key", "node", TTL)).isInstanceOf(CoordinationStorageException.class);
        var committed = lease.currentHolder("key").orElseThrow();
        assertThat(committed.holderId()).isEqualTo("node");
        assertThat(lease.tryAcquire("key", "node", TTL).orElseThrow()).isEqualTo(committed);
    }
    @Test void exhaustedVersionConflictIsNotSuccessfulUpdate() {
        registry.register(snapshot());
        var attempts = new AtomicInteger();
        var faulty = new ZooKeeperCoordinatorRegistry(inject((op, path, invocation) -> {
            if (op.equals("setData")) { attempts.incrementAndGet(); throw new KeeperException.BadVersionException(); }
            return invocation.call();
        }), "/registry");
        assertThatThrownBy(() -> faulty.updateLastProcessedInvalidationId("node", 99))
                .isInstanceOf(CoordinationStorageException.class).hasCauseInstanceOf(ConcurrentModificationException.class);
        assertThat(attempts).hasValue(3);
        assertThat(registry.getCoordinator("node").orElseThrow().lastProcessedInvalidationId()).isZero();
    }
    @Test void interruptedReadPreservesInterruptAndFailure() {
        var faulty = new ZooKeeperCoordinatorRegistry(inject((op, path, invocation) -> {
            if (op.equals("getData")) throw new InterruptedException("injected cancellation");
            return invocation.call();
        }), "/registry");
        try {
            assertThatThrownBy(() -> faulty.getCoordinator("node")).isInstanceOf(CoordinationStorageException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally { Thread.interrupted(); }
    }
    interface Invocation { Object call() throws Throwable; }
    interface Fault { Object apply(String operation, String path, Invocation invocation) throws Throwable; }
    CuratorFramework inject(Fault fault) { return (CuratorFramework) wrap(client, "", fault); }
    static Object wrap(Object target, String operation, Fault fault) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();
        for (Class<?> c = target.getClass(); c != null; c = c.getSuperclass()) {
            for (Class<?> i : c.getInterfaces()) if (Modifier.isPublic(i.getModifiers())) interfaces.add(i);
        }
        if (interfaces.isEmpty()) return target;
        return Proxy.newProxyInstance(CuratorFramework.class.getClassLoader(), interfaces.toArray(Class<?>[]::new),
                (proxy, method, args) -> {
                    String op = target instanceof CuratorFramework ? method.getName() : operation;
                    Invocation invoke = () -> {
                        try { return method.invoke(target, args); }
                        catch (InvocationTargetException failure) { throw failure.getCause(); }
                    };
                    Object result = method.getName().equals("forPath")
                            ? fault.apply(op, (String) args[0], invoke) : invoke.call();
                    if (result != null && result.getClass().getName().startsWith("org.apache.curator.framework.imps."))
                        return wrap(result, op, fault);
                    return result;
                });
    }
}
