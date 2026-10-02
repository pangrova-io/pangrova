package org.pangrova.observability.audit;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/** Real PostgreSQL delegates with bounded fault-injection gates for audit tests. */
final class JdbcAuditStoreTestDataSources {

    private JdbcAuditStoreTestDataSources() {}

    /**
     * A DataSource wrapper that can be toggled to throw SQLException on getConnection().
     */
    static final class FailableDataSource implements DataSource {

        private final DataSource delegate;
        private final AtomicBoolean failing = new AtomicBoolean(false);
        volatile Runnable beforeFailure = () -> {};

        FailableDataSource(DataSource delegate) {
            this.delegate = delegate;
        }

        void setFailing(boolean shouldFail) {
            failing.set(shouldFail);
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (failing.get()) {
                beforeFailure.run();
                throw new SQLException("Simulated transient DB error");
            }
            return delegate.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            if (failing.get()) {
                throw new SQLException("Simulated transient DB error");
            }
            return delegate.getConnection(username, password);
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Not a wrapper");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }

    /**
     * DataSource wrapper that tracks which thread calls getConnection().
     * Used to verify that save() does not perform JDBC on the calling thread.
     */
    static final class BlockingDataSource implements DataSource {

        private final DataSource delegate;
        private final CountDownLatch latch;
        private volatile Thread callerThread;
        private final AtomicBoolean callerSeenInJdbc = new AtomicBoolean(false);

        BlockingDataSource(DataSource delegate, CountDownLatch latch) {
            this.delegate = delegate;
            this.latch = latch;
        }

        void setCallerThread(Thread thread) {
            this.callerThread = thread;
        }

        boolean wasCallerThreadSeenInJdbc() {
            return callerSeenInJdbc.get();
        }

        @Override
        public Connection getConnection() throws SQLException {
            latch.countDown();
            if (Thread.currentThread() == callerThread) {
                callerSeenInJdbc.set(true);
            }
            return delegate.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() { return null; }
        @Override
        public void setLogWriter(PrintWriter out) {}
        @Override
        public void setLoginTimeout(int seconds) {}
        @Override
        public int getLoginTimeout() { return 0; }
        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Not a wrapper");
        }
        @Override
        public boolean isWrapperFor(Class<?> iface) { return false; }
    }

    /**
     * DataSource wrapper that waits for the caller's explicit release before JDBC.
     * Used to verify that save() does not block waiting for JDBC.
     */
    static final class GatedDataSource implements DataSource {

        private final DataSource delegate;
        private final CountDownLatch release;
        private final CountDownLatch entered;
        private volatile int heldMonitorCount;

        GatedDataSource(DataSource delegate, CountDownLatch release, CountDownLatch entered) {
            this.delegate = delegate;
            this.release = release;
            this.entered = entered;
        }

        int heldMonitorCount() {
            return heldMonitorCount;
        }

        @Override
        public Connection getConnection() throws SQLException {
            var info = ManagementFactory.getThreadMXBean().getThreadInfo(
                    new long[] {Thread.currentThread().getId()}, true, true)[0];
            heldMonitorCount = info.getLockedMonitors().length;
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new SQLException("Audit JDBC gate release timed out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted awaiting audit JDBC gate", e);
            }
            return delegate.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() { return null; }
        @Override
        public void setLogWriter(PrintWriter out) {}
        @Override
        public void setLoginTimeout(int seconds) {}
        @Override
        public int getLoginTimeout() { return 0; }
        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Not a wrapper");
        }
        @Override
        public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
