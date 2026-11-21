package org.pangrova.client.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CliBatchArgsTest {
    @TempDir Path directory;

    @Test
    void retainsBatchPathsAndSchema() {
        var args = PangrovaCli.parseArgs(new String[]{"--file", "q01.sql", "--schema", "tpch",
                "--output", "result.txt", "--execution-receipt", "receipt.json"});
        assertThat(args.batch.file()).isEqualTo(Path.of("q01.sql"));
        assertThat(args.schema).isEqualTo("tpch");
        assertThat(args.batch.output()).isEqualTo(Path.of("result.txt"));
        assertThat(args.batch.receipt()).isEqualTo(Path.of("receipt.json"));
    }

    @Test
    void rejectsUnknownDuplicateIncompleteAndMisplacedBatchOptions() {
        for (var args : new String[][]{{"--unknown", "value"}, {"--host", "a", "--host", "b"},
                {"--file"}, {"--file", "--schema", "tpch"}, {"q01.sql"}, {"--file", " "},
                {"--output", "result.txt"}, {"--execution-receipt", "receipt.json"},
                {"--file", "a.sql", "--output", "./a.sql"},
                {"--file", "a.sql", "--output", "result", "--execution-receipt", "result"}}) {
            assertThatThrownBy(() -> PangrovaCli.parseArgs(args)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void missingOrBlankFileFailsBeforeConnectingAndNeverWritesReceipt() throws Exception {
        var file = directory.resolve("query.sql");
        var receipt = directory.resolve("receipt.json");
        var args = new String[]{"--file", file.toString(), "--execution-receipt", receipt.toString()};
        assertThatThrownBy(() -> PangrovaCli.run(args, new BufferedReader(new StringReader("")), System.out))
                .isInstanceOf(java.io.UncheckedIOException.class);
        Files.writeString(file, " \n");
        assertThatThrownBy(() -> PangrovaCli.run(args, new BufferedReader(new StringReader("")), System.out))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("blank");
        assertThat(receipt).doesNotExist();
    }

    @Test
    void existingReceiptCannotBeReusedAsSuccess() throws Exception {
        var file = directory.resolve("query.sql");
        var receipt = directory.resolve("receipt.json");
        Files.writeString(file, "SELECT 1");
        Files.writeString(receipt, "prior execution");
        assertThatThrownBy(() -> PangrovaCli.run(new String[]{"--file", file.toString(),
                "--execution-receipt", receipt.toString()}, new BufferedReader(new StringReader("")), System.out))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("already exists");
        assertThat(Files.readString(receipt)).isEqualTo("prior execution");
    }
}
