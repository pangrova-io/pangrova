/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.ai.worker.function.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.ai.worker.blob.GovernedBlobStore;
import org.pangrova.ai.worker.blob.BlobTestStores;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileFromBytesFunctionTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void persistedReferenceBecomesFileStruct() {
        GovernedBlobStore store = BlobTestStores.create(
                temporaryDirectory, 1024, Set.of("image/png"));
        FileFromBytesFunction function = new FileFromBytesFunction(store);

        ColumnValue result = governed(() -> function.invoke(arguments()));

        assertThat(result).isInstanceOf(ColumnValue.RowValue.class);
        List<?> fields = ((ColumnValue.RowValue) result).fields();
        assertThat(fields.get(0)).asString().startsWith("pangrova-blob://v2/");
        assertThat(fields.get(1)).isEqualTo("image/png");
        assertThat(fields.get(2)).isEqualTo(3L);
        assertThat(fields.get(3)).asString().hasSize(64);
        assertThat(governed(() -> store.read(
                FileStructHelper.extractBlobReference((ColumnValue) result))))
                .containsExactly(1, 2, 3);
    }

    private static List<ColumnValue> arguments() {
        return List.of(
                new ColumnValue.BytesValue(new byte[]{1, 2, 3}),
                new ColumnValue.StringValue("image/png"));
    }

    private static <T> T governed(java.util.function.Supplier<T> action) {
        return org.pangrova.ai.worker.context.AiTestTaskAuthority.callWithRegisteredTask(
                "tenant-a", "alice", Instant.now().plus(Duration.ofMinutes(1)).toEpochMilli(),
                1024, action);
    }
}
