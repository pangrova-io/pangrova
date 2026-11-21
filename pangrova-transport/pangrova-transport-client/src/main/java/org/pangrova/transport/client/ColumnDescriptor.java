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

import io.substrait.proto.Type;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.LogicalType;

import java.sql.Types;
import java.util.Objects;
import java.util.Optional;

/**
 * Type-erased column descriptor carrying metadata needed for JDBC ResultSetMetaData.
 * Populated by {@link ArrowBatchDecoder} from the Arrow schema; contains no Arrow types.
 *
 * @param name      column name
 * @param sqlType   JDBC SQL type constant from {@link java.sql.Types}
 * @param typeName  server-side type name string
 * @param precision column precision (0 if not applicable)
 * @param scale     column scale (0 if not applicable)
 * @param logicalType complete canonical declaration when the field is a VECTOR
 */
public record ColumnDescriptor(String name, int sqlType, String typeName, int precision, int scale,
                               Optional<LogicalType> logicalType) {
    /** Requires VECTOR presentation metadata to agree with its complete schema declaration. */
    public ColumnDescriptor {
        Objects.requireNonNull(logicalType, "logicalType");
        if (logicalType.isPresent()) {
            LogicalType declaration = logicalType.orElseThrow();
            int dimension = VectorLogicalType.requireStorageDimension(declaration);
            if (sqlType != Types.ARRAY || !declaration.getCanonicalSql().equals(typeName)
                    || precision != dimension || scale != 0) {
                throw new IllegalArgumentException("VECTOR metadata differs from its canonical declaration");
            }
        } else if (typeName.startsWith("VECTOR(")) {
            throw new IllegalArgumentException("VECTOR metadata requires its canonical declaration");
        }
    }

    /** Java array representation actually emitted by the decoder for an annotated VECTOR column. */
    public Optional<Class<?>> vectorArrayClass() {
        return logicalType.map(declaration -> {
            var element = declaration.getSubstraitType().getList().getType();
            if (element.hasFp64()) {
                return element.getFp64().getNullability() == Type.Nullability.NULLABILITY_NULLABLE
                        ? Double[].class : double[].class;
            }
            return element.getFp32().getNullability() == Type.Nullability.NULLABILITY_NULLABLE
                    ? Float[].class : float[].class;
        });
    }

    /** Ordinary columns and nullable VECTOR rows permit nulls in result metadata. */
    public boolean allowsNull() {
        return logicalType.isEmpty() || logicalType.orElseThrow().getSubstraitType().getList().getNullability()
                != Type.Nullability.NULLABILITY_REQUIRED;
    }
}
