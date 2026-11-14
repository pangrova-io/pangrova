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
package org.pangrova.worker.execution;

import org.pangrova.protocol.execution.AiTaskLifecycle;

import java.util.ServiceLoader;

/** Resolves the optional AI task lifecycle installed by an AI runtime. */
final class AiTaskLifecycleRegistry {

    private static volatile AiTaskLifecycle lifecycle;

    private AiTaskLifecycleRegistry() {}

    static AiTaskLifecycle current() {
        AiTaskLifecycle resolved = lifecycle;
        if (resolved == null) {
            synchronized (AiTaskLifecycleRegistry.class) {
                resolved = lifecycle;
                if (resolved == null) {
                    resolved = ServiceLoader.load(AiTaskLifecycle.class)
                            .findFirst()
                            .orElse(AiTaskLifecycle.NONE);
                    lifecycle = resolved;
                }
            }
        }
        return resolved;
    }
}
