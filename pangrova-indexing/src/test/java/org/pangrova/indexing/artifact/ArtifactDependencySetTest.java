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
package org.pangrova.indexing.artifact;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.ArtifactDependency;
import org.pangrova.indexing.api.ArtifactDependencySet;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArtifactDependencySetTest {

    private static final String SHARED_VECTOR_SHA256 =
            "9e4739f5cda65f336924f1d0fc609b448cb2c5080df0f6c3ab7065d48af5ab79";

    @Test
    void ordersSharedVectorByUnsignedUtf8BytesAndHashesLengthPrefixedFields() {
        ArtifactDependencySet set = ArtifactDependencySet.of(List.of(
                dependency("\uD800\uDC00", "prefix", "v1"),
                dependency("\uE000", "prefix", "v1"),
                dependency("B", "id", "v1"),
                dependency("B", "i", "v2"),
                dependency("AA", "id", "v1"),
                dependency("A", "id", "v1")));

        assertThat(set.dependencies()).containsExactly(
                dependency("A", "id", "v1"),
                dependency("AA", "id", "v1"),
                dependency("B", "i", "v2"),
                dependency("B", "id", "v1"),
                dependency("\uE000", "prefix", "v1"),
                dependency("\uD800\uDC00", "prefix", "v1"));
        assertThat(set.sha256()).isEqualTo(SHARED_VECTOR_SHA256);
    }

    @Test
    void rejectsNulC1ControlAndMalformedUnicode() {
        assertThatThrownBy(() -> dependency("SOURCE\u0000SNAPSHOT", "id", "v1"))
                .hasMessageContaining("control");
        assertThatThrownBy(() -> dependency("SOURCE", "id\u0085", "v1"))
                .hasMessageContaining("control");
        assertThatThrownBy(() -> dependency("SOURCE", "id", "\uD800"))
                .hasMessageContaining("Unicode scalar");
    }

    @Test
    void rejectsDuplicateStructuralIdentityWithoutDelimiterKeys() {
        assertThatThrownBy(() -> ArtifactDependencySet.of(List.of(
                dependency("SOURCE", "table", "v1"),
                dependency("SOURCE", "table", "v2"))))
                .hasMessageContaining("duplicate logical identity");
    }

    private static ArtifactDependency dependency(
            String kind, String id, String version) {
        return new ArtifactDependency(kind, id, version);
    }
}
