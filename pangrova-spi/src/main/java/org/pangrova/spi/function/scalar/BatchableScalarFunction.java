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
package org.pangrova.spi.function.scalar;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.connector.data.value.ColumnValue;

import java.util.List;

/**
 * Extension of {@link ScalarFunction} that supports batch invocation.
 *
 * <p>When a function implements this interface, the execution engine may
 * accumulate multiple rows and invoke {@link #invokeBatch} instead of
 * calling {@link #invoke} per row. This enables amortization of expensive
 * per-invocation costs (e.g., HTTP round-trips to AI inference endpoints).</p>
 *
 * <p>The default implementation of {@link #invokeBatch} loops over
 * {@link #invoke}, so implementations that do not benefit from batching
 * need not override it.</p>
 */
@Experimental(reason = "AI batch optimization for reducing per-row HTTP overhead")
public interface BatchableScalarFunction extends ScalarFunction {

    /**
     * Invokes the function on a batch of argument lists.
     *
     * <p>Each element of {@code argumentsBatch} is the argument list for
     * one row. The returned list must have the same size and order.</p>
     *
     * @param argumentsBatch list of per-row argument lists
     * @return list of results, one per input row
     */
    default List<ColumnValue> invokeBatch(List<List<ColumnValue>> argumentsBatch) {
        return argumentsBatch.stream()
                .map(this::invoke)
                .toList();
    }
}
