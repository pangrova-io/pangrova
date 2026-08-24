package org.pangrova.worker.ai.blob;

import org.pangrova.spi.connector.index.FileSystemAccessor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory implementation of {@link FileSystemAccessor} for unit testing.
 * Stores all written content in a concurrent map keyed by path.
 */
final class InMemoryFileSystemAccessor implements FileSystemAccessor {

    private final ConcurrentMap<String, byte[]> files = new ConcurrentHashMap<>();
    private volatile boolean failOnWrite;
    private volatile boolean failOnRead;

    void setFailOnWrite(boolean fail) {
        this.failOnWrite = fail;
    }

    void setFailOnRead(boolean fail) {
        this.failOnRead = fail;
    }

    byte[] readAll(String path) {
        return files.get(path);
    }

    void openOutputStreamAndWrite(String path, byte[] content) {
        files.put(path, content.clone());
    }

    int fileCount() {
        return files.size();
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
        if (failOnRead) {
            throw new IOException("Simulated read failure for path: " + path);
        }
        byte[] data = files.get(path);
        if (data == null) {
            throw new IOException("File not found: " + path);
        }
        return new ByteArrayInputStream(data);
    }

    @Override
    public OutputStream openOutputStream(String path) throws IOException {
        if (failOnWrite) {
            throw new IOException("Simulated write failure for path: " + path);
        }
        return new CapturingOutputStream(path);
    }

    @Override
    public void deleteFile(String path) throws IOException {
        files.remove(path);
    }

    @Override
    public boolean exists(String path) throws IOException {
        return files.containsKey(path);
    }

    @Override
    public long fileSize(String path) throws IOException {
        byte[] data = files.get(path);
        if (data == null) {
            throw new IOException("File not found: " + path);
        }
        return data.length;
    }

    @Override
    public List<String> listFiles(String directoryPath) throws IOException {
        return files.keySet().stream()
                .filter(p -> p.startsWith(directoryPath))
                .toList();
    }

    @Override
    public void close() {
        files.clear();
    }

    private final class CapturingOutputStream extends ByteArrayOutputStream {
        private final String path;

        CapturingOutputStream(String path) {
            this.path = path;
        }

        @Override
        public void close() throws IOException {
            super.close();
            files.put(path, toByteArray());
        }
    }
}
