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
package org.pangrova.worker.ai.function.table;

import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.function.table.TableFunction;
import org.pangrova.spi.function.table.TableFunctionFactory;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Factory that registers AI-related table-valued functions.
 *
 * <p>Discovered via {@link ServiceLoader}. Provides the control-plane-visible
 * {@code DIRECTORY_LISTING} metadata function. Vector search is represented by
 * a typed canonical operator binding and executes exclusively in Native.</p>
 */
public final class AiTableFunctionFactory implements TableFunctionFactory {

    @Override
    public String getName() {
        return "ai-table-functions";
    }

    @Override
    public List<TableFunction> getTableFunctions(Map<String, String> config) {
        FileSystemAccessor storageAccessor = discoverStorageAccessor();
        return List.of(new DirectoryListingTableFunction(() -> storageAccessor));
    }

    private static FileSystemAccessor discoverStorageAccessor() {
        for (FileSystemAccessor accessor : ServiceLoader.load(FileSystemAccessor.class)) {
            return accessor;
        }
        return null;
    }
}
