package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.FileStorePathFactory;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.query.ConnectorQueryOperation;

import java.io.IOException;
import java.util.Objects;

/** Read-only planning view. The final physical operation releases the query permit. */
final class PaimonPlanningFileIO implements FileIO {
    private static final long serialVersionUID = 1L;

    private final FileIO delegate;
    private final transient PaimonScanBudget budget;
    private final transient ConnectorQueryOperation permit;
    private long bytesRemaining;
    private int manifestOpens;
    private int active;
    private volatile boolean sealed;
    private volatile ConnectorException rejection;

    PaimonPlanningFileIO(FileIO delegate, PaimonScanBudget budget, ConnectorQueryOperation permit, long bytes) {
        this.delegate = delegate;
        this.budget = budget;
        this.permit = permit;
        this.bytesRemaining = bytes;
    }

    synchronized ConnectorException reject(ConnectorException failure) {
        if (rejection == null) rejection = failure;
        return rejection;
    }

    ConnectorException rejection() {
        return rejection;
    }

    private synchronized void enter(boolean manifest) {
        check();
        requireByteBudget();
        if (manifest) {
            try {
                budget.checkMetadataCount((long) manifestOpens + 1, "manifest opens");
            } catch (ConnectorException failure) {
                throw reject(failure);
            }
            manifestOpens++;
        }
        active++;
    }

    void check() {
        ConnectorException failure = rejection;
        if (failure != null) throw failure;
        if (sealed) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED,
                    "Paimon metadata planning has already exited");
        }
        try {
            budget.check(0);
        } catch (ConnectorException cancelled) {
            throw reject(cancelled);
        }
    }

    private void exit() {
        boolean release;
        synchronized (this) {
            release = --active == 0 && sealed;
        }
        if (release) permit.close();
    }

    private synchronized int reserve(int requested) {
        check();
        requireByteBudget();
        int granted = (int) Math.min(bytesRemaining, requested);
        bytesRemaining -= granted;
        return granted;
    }

    private void requireByteBudget() {
        if (bytesRemaining == 0) {
            throw reject(new ConnectorException(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED,
                    "Paimon encoded metadata read budget exhausted"));
        }
    }

    private synchronized void refund(int unused) {
        bytesRemaining += unused;
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        String name = path.getName();
        boolean manifest = name.startsWith(FileStorePathFactory.INDEX_MANIFEST_PREFIX)
                || name.startsWith(FileStorePathFactory.MANIFEST_PREFIX)
                        && !name.startsWith(FileStorePathFactory.MANIFEST_LIST_PREFIX);
        enter(manifest);
        try {
            return new PlannedInput(delegate.newInputStream(path));
        } catch (IOException | RuntimeException | Error failure) {
            exit();
            throw failure;
        }
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        enter(false);
        try {
            return delegate.getFileStatus(path);
        } finally {
            exit();
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        enter(false);
        try {
            return delegate.listStatus(path);
        } finally {
            exit();
        }
    }

    @Override
    public boolean exists(Path path) throws IOException {
        enter(false);
        try {
            return delegate.exists(path);
        } finally {
            exit();
        }
    }

    @Override
    public boolean isObjectStore() { return delegate.isObjectStore(); }

    @Override
    public void configure(CatalogContext context) { throw readOnly(); }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) { throw readOnly(); }

    @Override
    public boolean delete(Path path, boolean recursive) { throw readOnly(); }

    @Override
    public boolean mkdirs(Path path) { throw readOnly(); }

    @Override
    public boolean rename(Path source, Path target) { throw readOnly(); }

    private static UnsupportedOperationException readOnly() {
        return new UnsupportedOperationException("Paimon metadata planning is read-only");
    }

    @Override
    public void close() {
        boolean release;
        synchronized (this) {
            if (sealed) return;
            sealed = true;
            release = active == 0;
        }
        if (release) permit.close();
    }

    private final class PlannedInput extends SeekableInputStream {
        private final SeekableInputStream input;
        private boolean closed;

        private PlannedInput(SeekableInputStream input) {
            this.input = input;
        }

        @Override
        public void seek(long position) throws IOException {
            check();
            input.seek(position);
        }

        @Override
        public long getPos() throws IOException { return input.getPos(); }

        @Override
        public int read() throws IOException {
            reserve(1);
            int result = -1;
            try {
                result = input.read();
                return result;
            } finally {
                if (result < 0) refund(1);
            }
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, buffer.length);
            if (length == 0) return 0;
            int granted = reserve(length);
            int result = 0;
            try {
                result = input.read(buffer, offset, granted);
                return result;
            } finally {
                refund(granted - Math.max(result, 0));
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) return;
            closed = true;
            try {
                input.close();
            } finally {
                exit();
            }
        }
    }
}
