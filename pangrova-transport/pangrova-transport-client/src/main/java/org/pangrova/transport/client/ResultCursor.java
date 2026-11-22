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
package org.pangrova.transport.client;

import java.util.List;

/**
 * Forward-only result with a bounded transport window. Read and close from a single consumer.
 * Values are detached from the transport buffers and remain usable after advancing the cursor.
 */
public interface ResultCursor extends AutoCloseable {

    List<ColumnDescriptor> columns();

    boolean next();

    /** Returns the value of the zero-based column in the current row. */
    Object value(int column);

    @Override
    void close();
}
