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
package org.pangrova.transport;

/**
 * Type-erased column descriptor carrying metadata needed for JDBC ResultSetMetaData.
 * Populated by {@link ArrowBatchDecoder} from the Arrow schema; contains no Arrow types.
 *
 * @param name      column name
 * @param sqlType   JDBC SQL type constant from {@link java.sql.Types}
 * @param typeName  server-side type name string
 * @param precision column precision (0 if not applicable)
 * @param scale     column scale (0 if not applicable)
 */
public record ColumnDescriptor(String name, int sqlType, String typeName, int precision, int scale) {}
