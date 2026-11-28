package org.pangrova.connector.paimon.write;

import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.pangrova.connector.api.enums.CommitEventType;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;

/** SDK messages and exact observations of staged data objects under the bound table root. */
public final class PaimonSdkWriteFragment {
    private static final int FRAGMENT_VERSION_WITHOUT_OBJECT_IDENTITIES = 2;
    private static final int FRAGMENT_VERSION = 3;

    private PaimonSdkWriteFragment() {}

    public record StagedObject(String uri, long sizeBytes, String localFingerprint) {}

    public record Decoded(CommitEventType eventType, List<CommitMessage> messages,
                          List<StagedObject> objects, String localAuthority) {
        public Decoded {
            messages = List.copyOf(messages);
            objects = List.copyOf(objects);
        }
    }

    static byte[] encode(FileStoreTable table, PaimonOutputTableHandle handle, CommitEventType event,
            List<CommitMessage> messages) throws IOException {
        var serializer = new CommitMessageSerializer();
        var objects = collectObjects(table, handle, messages);
        try (var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes)) {
            out.writeInt(FRAGMENT_VERSION);
            var allocation = handle.stagedFileNames().allocationId();
            out.writeLong(allocation.getMostSignificantBits());
            out.writeLong(allocation.getLeastSignificantBits());
            out.writeUTF(event.name());
            out.writeUTF(handle.schemaName());
            out.writeUTF(handle.tableName());
            var root = PaimonLocalStagingAuthority.absolutePath(new Path(handle.tableLocation()).toUri());
            out.writeUTF(!objects.isEmpty() && root != null
                    ? PaimonLocalStagingAuthority.observe(root.getParent()) : "");
            out.writeInt(serializer.getVersion());
            out.writeInt(messages.size());
            for (var message : messages) {
                byte[] encoded = serializer.serialize(message);
                out.writeInt(encoded.length);
                out.write(encoded);
            }
            out.writeInt(objects.size());
            for (var object : objects.values()) {
                out.writeUTF(object.uri());
                out.writeUTF(object.localFingerprint());
                out.writeLong(object.sizeBytes());
            }
            out.flush();
            return bytes.toByteArray();
        }
    }

    /** Reads and validates one fragment against its immutable allocation, without contacting the catalog. */
    public static Decoded decode(byte[] fragment, PaimonOutputTableHandle handle) throws IOException {
        try (var in = new DataInputStream(new ByteArrayInputStream(fragment))) {
            int version = in.readInt();
            if (version != FRAGMENT_VERSION && version != FRAGMENT_VERSION_WITHOUT_OBJECT_IDENTITIES) {
                throw new IllegalArgumentException("Unsupported Paimon commit fragment version");
            }
            if (!new UUID(in.readLong(), in.readLong()).equals(handle.stagedFileNames().allocationId())) {
                throw new IllegalArgumentException("Paimon fragment differs from its staged file allocation");
            }
            CommitEventType event = CommitEventType.valueOf(in.readUTF());
            if (!handle.schemaName().equals(in.readUTF()) || !handle.tableName().equals(in.readUTF())) {
                throw new IllegalArgumentException("Paimon fragment target differs from the bound table");
            }
            String localAuthority = in.readUTF();
            if (!localAuthority.isEmpty() && !localAuthority.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("Invalid Paimon local staging authority");
            }
            var messages = readMessages(in, in.readInt());
            var objects = readObjects(in, handle, messages, version);
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in Paimon commit fragment");
            }
            return new Decoded(event, messages, objects, localAuthority);
        }
    }

    private static List<CommitMessage> readMessages(DataInputStream in, int version) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > in.available() / Integer.BYTES) {
            throw new IllegalArgumentException("Invalid Paimon commit message count");
        }
        var serializer = new CommitMessageSerializer();
        var messages = new ArrayList<CommitMessage>();
        for (int i = 0; i < count; i++) {
            int length = in.readInt();
            if (length < 0 || length > in.available()) {
                throw new IllegalArgumentException("Invalid Paimon commit message length");
            }
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            messages.add(serializer.deserialize(version, bytes));
        }
        return messages;
    }

    private static List<StagedObject> readObjects(DataInputStream in, PaimonOutputTableHandle handle,
            List<CommitMessage> messages, int version) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > in.available() / (Short.BYTES + Long.BYTES)) {
            throw new IllegalArgumentException("Invalid Paimon staged object count");
        }
        if (count == 0) return List.of();
        var alloc = handle.stagedFileNames();
        var names = new HashMap<String, Long>();
        for (var message : messages) {
            for (var file : publicationFiles((CommitMessageImpl) message)) {
                if (belongsToAllocation(file, alloc)) {
                    names.merge(file.fileName(), file.fileSize(), (previous, current) -> {
                        if (!previous.equals(current)) {
                            throw new IllegalArgumentException("Conflicting Paimon staged object lengths");
                        }
                        return previous;
                    });
                }
            }
        }
        URI root = new Path(handle.tableLocation()).toUri().normalize();
        var objects = new ArrayList<StagedObject>();
        var observedNames = new HashSet<String>();
        String previous = null;
        for (int i = 0; i < count; i++) {
            String location = in.readUTF();
            String fingerprint = version == FRAGMENT_VERSION ? in.readUTF() : "";
            if (!fingerprint.isEmpty() && !fingerprint.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("Invalid Paimon staged object fingerprint");
            }
            long size = in.readLong();
            URI uri = URI.create(location);
            if (!beneathTable(root, uri) || (previous != null && previous.compareTo(location) >= 0) || size < 0) {
                throw new IllegalArgumentException("Invalid Paimon staged object evidence");
            }
            String name = new Path(uri).getName();
            if (!observedNames.add(name) || !Objects.equals(names.get(name), size)) {
                throw new IllegalArgumentException("Paimon staged object differs from its SDK commit metadata");
            }
            objects.add(new StagedObject(location, size, fingerprint));
            previous = location;
        }
        return objects;
    }

    private static Map<String, StagedObject> collectObjects(FileStoreTable table, PaimonOutputTableHandle handle,
            List<CommitMessage> messages) throws IOException {
        var objects = new TreeMap<String, StagedObject>();
        var alloc = handle.stagedFileNames();
        URI root = new Path(handle.tableLocation()).toUri().normalize();
        var pathFactory = table.store().pathFactory();
        for (var encoded : messages) {
            var message = (CommitMessageImpl) encoded;
            var paths = pathFactory.createDataFilePathFactory(message.partition(), message.bucket());
            for (var file : publicationFiles(message)) {
                if (belongsToAllocation(file, alloc)) {
                    recordObject(root, paths, file, objects);
                }
            }
        }
        return objects;
    }

    private static List<DataFileMeta> publicationFiles(CommitMessageImpl message) {
        var append = message.newFilesIncrement();
        var compact = message.compactIncrement();
        var files = new ArrayList<>(append.newFiles());
        files.addAll(append.changelogFiles());
        files.addAll(compact.compactAfter());
        files.addAll(compact.changelogFiles());
        return files;
    }

    private static boolean belongsToAllocation(DataFileMeta file, PaimonOutputTableHandle.StagedFileNames alloc) {
        return file.fileName().startsWith(alloc.dataPrefix()) || file.fileName().startsWith(alloc.changelogPrefix());
    }

    private static void recordObject(URI root, DataFilePathFactory paths, DataFileMeta file,
            Map<String, StagedObject> objects) throws IOException {
        var path = paths.toPath(file);
        var uri = path.toUri();
        if (!beneathTable(root, uri) || !path.getName().equals(file.fileName())) {
            return;
        }
        String location = uri.toASCIIString();
        String fingerprint = PaimonLocalStagingAuthority.observeObject(
                PaimonLocalStagingAuthority.absolutePath(uri), file.fileSize());
        var receipt = new StagedObject(location, file.fileSize(), fingerprint);
        var previous = objects.putIfAbsent(location, receipt);
        if (previous != null && !previous.equals(receipt)) {
            throw new IllegalArgumentException("Conflicting Paimon staged object evidence");
        }
    }

    private static boolean beneathTable(URI root, URI object) {
        String parent = root.getPath();
        String child = object.getPath();
        return parent != null && !parent.isEmpty() && child != null && object.getQuery() == null
                && object.getFragment() == null && object.getUserInfo() == null && object.normalize().equals(object)
                && Objects.equals(root.getScheme(), object.getScheme())
                && Objects.equals(root.getAuthority(), object.getAuthority())
                && child.startsWith(parent.endsWith("/") ? parent : parent + "/")
                && !child.endsWith("/") && !child.contains("/../") && !child.contains("/./");
    }
}
