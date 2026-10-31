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
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.ai.worker.blob.GovernedBlobStore;
import org.pangrova.ai.worker.blob.HttpsBlobImporter;
import org.pangrova.ai.worker.blob.BlobTestStores;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileFromUrlFunctionTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void preservesSqlNullWithoutNetworkAccess() {
        ColumnValue result = function().invoke(List.of(new ColumnValue.NullValue()));

        assertThat(result).isInstanceOf(ColumnValue.NullValue.class);
    }

    @Test
    void rejectsUnsafeInputBeforeCallingResolver() {
        FileFromUrlFunction function = function();

        assertThatThrownBy(() -> function.invoke(arguments("file:///etc/passwd")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HTTPS");
    }

    private static List<ColumnValue> arguments(String uri) {
        return List.of(new ColumnValue.StringValue(uri));
    }

    private FileFromUrlFunction function() {
        GovernedBlobStore store = BlobTestStores.create(
                temporaryDirectory, 1024, Set.of("image/png"));
        return new FileFromUrlFunction(new HttpsBlobImporter(
                store, new HttpsBlobImporter.Settings(
                        Set.of(), Set.of("image/png"),
                        Duration.ofSeconds(1), Duration.ofSeconds(1), 1024, 1024,
                        new HttpsBlobImporter.PoolSettings(
                                8, 4, Duration.ofMinutes(1),
                                Duration.ofSeconds(10)))));
    }
}
