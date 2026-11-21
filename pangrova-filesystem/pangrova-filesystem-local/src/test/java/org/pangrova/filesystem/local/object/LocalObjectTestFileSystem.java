package org.pangrova.filesystem.local.object;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;

/** Delegates every operation to real NIO files; callbacks observe exact publication and deletion boundaries. */
final class LocalObjectTestFileSystem extends FileSystemProvider {
    private final FileSystem delegate = FileSystems.getDefault();
    private final FileSystemProvider provider = delegate.provider();
    private final View view = new View();
    IoAction afterMove = path -> { };
    IoAction beforeDelete = path -> { };
    IoAction beforeChannel = path -> { };
    IoAction beforeDirectory = path -> { };
    IoAction beforeRealPath = path -> { };

    @FunctionalInterface interface IoAction { void accept(Path path) throws IOException; }

    Path wrap(Path path) { return new Entry(path); }
    private Path unwrap(Path path) { return path instanceof Entry entry ? entry.path : path; }
    @Override public String getScheme() { return "local-object-test"; }
    @Override public FileSystem newFileSystem(URI uri, Map<String, ?> env) { return view; }
    @Override public FileSystem getFileSystem(URI uri) { return view; }
    @Override public Path getPath(URI uri) { return wrap(Path.of(uri)); }
    @Override public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options,
            FileAttribute<?>... attrs) throws IOException { return provider.newByteChannel(unwrap(path), options, attrs); }
    @Override public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options,
            FileAttribute<?>... attrs) throws IOException {
        beforeChannel.accept(unwrap(path));
        return provider.newFileChannel(unwrap(path), options, attrs);
    }
    @Override public DirectoryStream<Path> newDirectoryStream(Path path,
            DirectoryStream.Filter<? super Path> filter) throws IOException {
        var stream = provider.newDirectoryStream(unwrap(path), entry -> filter.accept(wrap(entry)));
        return new DirectoryStream<>() {
            @Override public Iterator<Path> iterator() {
                var entries = stream.iterator();
                return new Iterator<>() {
                    @Override public boolean hasNext() { return entries.hasNext(); }
                    @Override public Path next() { return wrap(entries.next()); }
                };
            }
            @Override public void close() throws IOException { stream.close(); }
        };
    }
    @Override public void createDirectory(Path path, FileAttribute<?>... attrs) throws IOException {
        beforeDirectory.accept(unwrap(path));
        provider.createDirectory(unwrap(path), attrs);
    }
    @Override public void delete(Path path) throws IOException {
        beforeDelete.accept(unwrap(path));
        provider.delete(unwrap(path));
    }
    @Override public boolean deleteIfExists(Path path) throws IOException {
        beforeDelete.accept(unwrap(path));
        return provider.deleteIfExists(unwrap(path));
    }
    @Override public void copy(Path source, Path target, CopyOption... options) throws IOException {
        provider.copy(unwrap(source), unwrap(target), options);
    }
    @Override public void move(Path source, Path target, CopyOption... options) throws IOException {
        provider.move(unwrap(source), unwrap(target), options);
        afterMove.accept(unwrap(target));
    }
    @Override public boolean isSameFile(Path first, Path second) throws IOException {
        return provider.isSameFile(unwrap(first), unwrap(second));
    }
    @Override public boolean isHidden(Path path) throws IOException { return provider.isHidden(unwrap(path)); }
    @Override public FileStore getFileStore(Path path) throws IOException { return provider.getFileStore(unwrap(path)); }
    @Override public void checkAccess(Path path, AccessMode... modes) throws IOException {
        provider.checkAccess(unwrap(path), modes);
    }
    @Override public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type,
            LinkOption... options) { return provider.getFileAttributeView(unwrap(path), type, options); }
    @Override public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type,
            LinkOption... options) throws IOException { return provider.readAttributes(unwrap(path), type, options); }
    @Override public Map<String, Object> readAttributes(Path path, String attributes,
            LinkOption... options) throws IOException { return provider.readAttributes(unwrap(path), attributes, options); }
    @Override public void setAttribute(Path path, String attribute, Object value, LinkOption... options)
            throws IOException { provider.setAttribute(unwrap(path), attribute, value, options); }

    private final class View extends FileSystem {
        @Override public FileSystemProvider provider() { return LocalObjectTestFileSystem.this; }
        @Override public void close() { }
        @Override public boolean isOpen() { return true; }
        @Override public boolean isReadOnly() { return delegate.isReadOnly(); }
        @Override public String getSeparator() { return delegate.getSeparator(); }
        @Override public Iterable<Path> getRootDirectories() {
            var roots = new ArrayList<Path>(); delegate.getRootDirectories().forEach(path -> roots.add(wrap(path)));
            return roots;
        }
        @Override public Iterable<FileStore> getFileStores() { return delegate.getFileStores(); }
        @Override public Set<String> supportedFileAttributeViews() { return delegate.supportedFileAttributeViews(); }
        @Override public Path getPath(String first, String... more) { return wrap(delegate.getPath(first, more)); }
        @Override public PathMatcher getPathMatcher(String syntax) {
            var matcher = delegate.getPathMatcher(syntax); return path -> matcher.matches(unwrap(path));
        }
        @Override public UserPrincipalLookupService getUserPrincipalLookupService() {
            return delegate.getUserPrincipalLookupService();
        }
        @Override public WatchService newWatchService() throws IOException { return delegate.newWatchService(); }
    }

    private final class Entry implements Path {
        private final Path path;
        Entry(Path path) { this.path = path; }
        @Override public FileSystem getFileSystem() { return view; }
        @Override public boolean isAbsolute() { return path.isAbsolute(); }
        @Override public Path getRoot() { return path.getRoot() == null ? null : wrap(path.getRoot()); }
        @Override public Path getFileName() { return path.getFileName() == null ? null : wrap(path.getFileName()); }
        @Override public Path getParent() { return path.getParent() == null ? null : wrap(path.getParent()); }
        @Override public int getNameCount() { return path.getNameCount(); }
        @Override public Path getName(int index) { return wrap(path.getName(index)); }
        @Override public Path subpath(int begin, int end) { return wrap(path.subpath(begin, end)); }
        @Override public boolean startsWith(Path other) { return path.startsWith(unwrap(other)); }
        @Override public boolean endsWith(Path other) { return path.endsWith(unwrap(other)); }
        @Override public Path normalize() { return wrap(path.normalize()); }
        @Override public Path resolve(Path other) { return wrap(path.resolve(unwrap(other))); }
        @Override public Path relativize(Path other) { return wrap(path.relativize(unwrap(other))); }
        @Override public URI toUri() { return path.toUri(); }
        @Override public Path toAbsolutePath() { return wrap(path.toAbsolutePath()); }
        @Override public Path toRealPath(LinkOption... options) throws IOException {
            beforeRealPath.accept(path);
            return wrap(path.toRealPath(options));
        }
        @Override public java.io.File toFile() { return path.toFile(); }
        @Override public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events,
                WatchEvent.Modifier... modifiers) throws IOException { return path.register(watcher, events, modifiers); }
        @Override public Iterator<Path> iterator() {
            var entries = path.iterator();
            return new Iterator<>() {
                @Override public boolean hasNext() { return entries.hasNext(); }
                @Override public Path next() { return wrap(entries.next()); }
            };
        }
        @Override public int compareTo(Path other) { return path.compareTo(unwrap(other)); }
        @Override public boolean equals(Object other) { return other instanceof Entry entry && path.equals(entry.path); }
        @Override public int hashCode() { return path.hashCode(); }
        @Override public String toString() { return path.toString(); }
    }
}
