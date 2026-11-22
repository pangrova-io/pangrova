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
package org.pangrova.client.jdbc;

import org.pangrova.transport.client.ColumnDescriptor;
import org.pangrova.transport.client.ResultBatch;
import org.pangrova.transport.client.ResultCursor;
import org.pangrova.transport.client.exception.TransportException;

import java.sql.SQLException;
import java.util.List;

final class ResultSetTestFixtures {
    private ResultSetTestFixtures() {}

    static PangrovaResultSet fromBatch(ResultBatch batch) {
        return new PangrovaResultSet(new ResultCursor() {
            private final List<Object[]> rows = batch.rows();
            private int row = -1;

            @Override public List<ColumnDescriptor> columns() { return batch.columns(); }
            @Override public boolean next() { return ++row < rows.size(); }
            @Override public Object value(int column) { return rows.get(row)[column]; }
            @Override public void close() {}
        }, null, new PangrovaResultSet.Completion() {
            @Override public void finish() {}
            @Override public void abandon() {}
            @Override public SQLException translate(TransportException failure) { return new SQLException(failure); }
        });
    }
}
