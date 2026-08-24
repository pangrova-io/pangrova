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
package org.pangrova.spi.execution.interop;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class NativeBufferOwnershipTest {

    @Test
    void copiedIsNotZeroCopy() {
        assertThat(NativeBufferOwnership.COPIED.isZeroCopy()).isFalse();
    }

    @Test
    void copiedIsReleaseSafe() {
        assertThat(NativeBufferOwnership.COPIED.isReleaseSafe()).isTrue();
    }

    @Test
    void javaReleaseCallbackIsZeroCopy() {
        assertThat(NativeBufferOwnership.JAVA_RELEASE_CALLBACK.isZeroCopy()).isTrue();
    }

    @Test
    void javaReleaseCallbackIsNotReleaseSafe() {
        assertThat(NativeBufferOwnership.JAVA_RELEASE_CALLBACK.isReleaseSafe()).isFalse();
    }

    @Test
    void nativeOwnedHandleIsZeroCopy() {
        assertThat(NativeBufferOwnership.NATIVE_OWNED_HANDLE.isZeroCopy()).isTrue();
    }

    @Test
    void nativeOwnedHandleIsReleaseSafe() {
        assertThat(NativeBufferOwnership.NATIVE_OWNED_HANDLE.isReleaseSafe()).isTrue();
    }

    @Test
    void recommendedIsNativeOwnedHandle() {
        assertThat(NativeBufferOwnership.recommended())
                .isEqualTo(NativeBufferOwnership.NATIVE_OWNED_HANDLE);
    }

    @Test
    void nativeOwnedHandleIsBothZeroCopyAndSafe() {
        NativeBufferOwnership model = NativeBufferOwnership.NATIVE_OWNED_HANDLE;
        assertThat(model.isZeroCopy() && model.isReleaseSafe())
                .as("NATIVE_OWNED_HANDLE should be both zero-copy and release-safe")
                .isTrue();
    }

    @Test
    void noModelIsBothUnsafeAndCopied() {
        for (NativeBufferOwnership model : NativeBufferOwnership.values()) {
            assertThat(model.isZeroCopy() || model.isReleaseSafe())
                    .as("Every ownership model should be either zero-copy or release-safe (or both)")
                    .isTrue();
        }
    }

    @ParameterizedTest
    @EnumSource(NativeBufferOwnership.class)
    void allModelsHaveConsistentNames(NativeBufferOwnership model) {
        assertThat(NativeBufferOwnership.valueOf(model.name())).isEqualTo(model);
    }

    @Test
    void exactlyThreeModels() {
        assertThat(NativeBufferOwnership.values()).hasSize(3);
    }
}
