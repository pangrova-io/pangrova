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
package org.pangrova.protocol.execution;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;

/**
 * Worker-owned task admission and accounting seam for AI provider capacity.
 *
 * <p>The Worker registers every Native task before execution and merges the
 * provider resource usage into the terminal report. The AI runtime supplies the
 * implementation so that the Worker core remains usable without any AI module
 * on the classpath; the disabled implementation owns no admission state and
 * reports no provider usage.</p>
 *
 * <p>A Worker installs at most one implementation of this contract. Discovery constructors
 * are resource-free. The Worker may invoke the installed service concurrently.</p>
 */
@Experimental
public interface AiTaskLifecycle {

    /** Installs the canonical task authority before any Native callback can execute. */
    void registerTask(TaskRequest request);

    /** Retires transient provider admission state after the Native task joins. */
    ResourceUsage releaseTask(String tenantId, TaskAttemptId attempt);

    /** Revokes new provider sends immediately while retaining evidence until joined release. */
    void cancelTask(String tenantId, TaskAttemptId attempt);

    /** Returns kernel resource counters for the exact live task attempt. */
    ResourceUsage usage(String tenantId, TaskAttemptId attempt);

    /** No-op lifecycle used when no AI runtime is installed. */
    AiTaskLifecycle NONE = new AiTaskLifecycle() {
        @Override
        public void registerTask(TaskRequest request) {
        }

        @Override
        public ResourceUsage releaseTask(String tenantId, TaskAttemptId attempt) {
            return ResourceUsage.getDefaultInstance();
        }

        @Override
        public void cancelTask(String tenantId, TaskAttemptId attempt) {
        }

        @Override
        public ResourceUsage usage(String tenantId, TaskAttemptId attempt) {
            return ResourceUsage.getDefaultInstance();
        }
    };
}
