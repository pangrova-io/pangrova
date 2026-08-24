package org.pangrova.connector.paimon.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test verifying Paimon CommitMessage serialization roundtrip.
 *
 * <p>For any valid CommitMessage list produced by Paimon's BatchTableWrite,
 * serializing via the same binary format used by AbstractPaimonPageSink and
 * deserializing via the same logic used by PaimonMetadata.finishDistributedWrite()
 * should restore an equivalent set of CommitMessages.</p>
 *
 * <p>Validates: Requirements 2.3, 6.1</p>
 */
class PaimonCommitMessagePropertyTest {

    private static final String DATABASE = "prop_db";
    private static final String TABLE_NAME = "commit_roundtrip";

    private Path warehouse;
    private Catalog catalog;
    private Table table;

    @BeforeProperty
    void setUp() throws Exception {
        warehouse = Files.createTempDirectory("paimon-commit-prop-");
        Options options = new Options();
        options.setString("warehouse", warehouse.toString());
        CatalogContext context = CatalogContext.create(options);
        catalog = CatalogFactory.createCatalog(context);
        catalog.createDatabase(DATABASE, false);

        Schema schema = Schema.newBuilder()
                .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                .column("name", org.apache.paimon.types.DataTypes.STRING())
                .column("value", org.apache.paimon.types.DataTypes.DOUBLE())
                .primaryKey("id")
                .option("bucket", "1")
                .build();
        Identifier tableId = Identifier.create(DATABASE, TABLE_NAME);
        catalog.createTable(tableId, schema, false);
        table = catalog.getTable(tableId);
    }

    @AfterProperty
    void tearDown() throws Exception {
        if (warehouse != null) {
            Files.walk(warehouse)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) { }
                    });
        }
    }

    /**
     * For any valid CommitMessage list, serializing then deserializing
     * should restore an equivalent set.
     *
     * <p>Validates: Requirements 2.3, 6.1</p>
     */
    @Property(tries = 10)
    void commitMessageRoundtrip(@ForAll("rowCounts") int rowCount) throws Exception {
        List<CommitMessage> original = produceCommitMessages(rowCount);

        byte[] serialized = serializeCommitMessages(original, DATABASE, TABLE_NAME, "APPEND");
        List<CommitMessage> deserialized = deserializeCommitMessages(serialized);

        assertThat(deserialized).hasSameSizeAs(original);

        // Re-serialize both sides and compare bytes for equivalence,
        // since CommitMessage does not implement equals()
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        for (int i = 0; i < original.size(); i++) {
            byte[] origBytes = serializer.serialize(original.get(i));
            byte[] roundtripBytes = serializer.serialize(deserialized.get(i));
            assertThat(roundtripBytes)
                    .as("CommitMessage[%d] should be byte-equivalent after roundtrip", i)
                    .isEqualTo(origBytes);
        }
    }

    /**
     * Empty CommitMessage list should serialize and deserialize to empty.
     *
     * <p>Validates: Requirements 2.3, 6.1</p>
     */
    @Property(tries = 5)
    void emptyCommitMessageListRoundtrip(@ForAll("operations") String operation) throws Exception {
        List<CommitMessage> empty = List.of();

        byte[] serialized = serializeCommitMessages(empty, DATABASE, TABLE_NAME, operation);
        List<CommitMessage> deserialized = deserializeCommitMessages(serialized);

        assertThat(deserialized).isEmpty();
    }

    /**
     * Multiple fragments (simulating multi-worker scenario) should all
     * deserialize correctly when concatenated.
     *
     * <p>Validates: Requirements 2.3, 6.1</p>
     */
    @Property(tries = 5)
    void multiFragmentRoundtrip(@ForAll("smallRowCounts") int countA,
                                @ForAll("smallRowCounts") int countB) throws Exception {
        List<CommitMessage> messagesA = produceCommitMessages(countA);
        List<CommitMessage> messagesB = produceCommitMessages(countB);

        byte[] fragmentA = serializeCommitMessages(messagesA, DATABASE, TABLE_NAME, "APPEND");
        byte[] fragmentB = serializeCommitMessages(messagesB, DATABASE, TABLE_NAME, "APPEND");

        List<CommitMessage> allDeserialized = new ArrayList<>();
        allDeserialized.addAll(deserializeCommitMessages(fragmentA));
        allDeserialized.addAll(deserializeCommitMessages(fragmentB));

        assertThat(allDeserialized).hasSize(messagesA.size() + messagesB.size());

        // Verify byte-level equivalence via re-serialization
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<CommitMessage> allOriginal = new ArrayList<>(messagesA);
        allOriginal.addAll(messagesB);
        for (int i = 0; i < allOriginal.size(); i++) {
            byte[] origBytes = serializer.serialize(allOriginal.get(i));
            byte[] roundtripBytes = serializer.serialize(allDeserialized.get(i));
            assertThat(roundtripBytes)
                    .as("Fragment CommitMessage[%d] byte-equivalent after roundtrip", i)
                    .isEqualTo(origBytes);
        }
    }

    // ── Arbitraries ──

    @Provide
    Arbitrary<Integer> rowCounts() {
        return Arbitraries.integers().between(0, 20);
    }

    @Provide
    Arbitrary<Integer> smallRowCounts() {
        return Arbitraries.integers().between(0, 5);
    }

    @Provide
    Arbitrary<String> operations() {
        return Arbitraries.of("APPEND", "OVERWRITE");
    }

    // ── Helpers: replicate production serialization/deserialization logic ──

    /**
     * Produces real CommitMessages by writing rows to the Paimon table
     * and calling prepareCommit(). This mirrors what AbstractPaimonPageSink does.
     */
    private List<CommitMessage> produceCommitMessages(int rowCount) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            for (int i = 0; i < rowCount; i++) {
                write.write(GenericRow.of(
                        (long) (System.nanoTime() + i),
                        BinaryString.fromString("row_" + i),
                        (double) i));
            }
            return write.prepareCommit();
        }
    }

    /**
     * Replicates the binary format from AbstractPaimonPageSink.serializeCommitMessages():
     * [version:int][operation:utf][schema:utf][table:utf][count:int][msg_len:int][msg_bytes]...
     */
    private static byte[] serializeCommitMessages(List<CommitMessage> messages,
                                                  String schemaName,
                                                  String tableName,
                                                  String operation) throws IOException {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(serializer.getVersion());
            out.writeUTF(operation);
            out.writeUTF(schemaName);
            out.writeUTF(tableName);
            out.writeInt(messages.size());
            for (CommitMessage msg : messages) {
                byte[] msgBytes = serializer.serialize(msg);
                out.writeInt(msgBytes.length);
                out.write(msgBytes);
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    /**
     * Replicates the deserialization logic from PaimonMetadata.finishDistributedWrite():
     * reads the binary envelope and deserializes each CommitMessage.
     */
    private static List<CommitMessage> deserializeCommitMessages(byte[] fragment) throws IOException {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<CommitMessage> messages = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(fragment))) {
            int version = in.readInt();
            String operation = in.readUTF();
            String schemaName = in.readUTF();
            String tableName = in.readUTF();
            int messageCount = in.readInt();
            for (int i = 0; i < messageCount; i++) {
                int msgLen = in.readInt();
                byte[] msgBytes = new byte[msgLen];
                in.readFully(msgBytes);
                messages.add(serializer.deserialize(version, msgBytes));
            }
        }
        return messages;
    }
}
