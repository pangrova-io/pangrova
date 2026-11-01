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
package org.pangrova.ai.worker.context;

import org.pangrova.spi.function.runtime.FunctionCallbackScope;

import java.util.function.Supplier;

/** Binds the canonical Native callback authority for AI SQL function evaluation. */
public final class WorkerAiCallbackScope implements FunctionCallbackScope {

    @Override
    public <T> T call(byte[] canonicalExecutionContext, Supplier<T> callback) {
        return AiExecutionContext.callWith(
                AiExecutionContext.fromCanonicalCallback(canonicalExecutionContext), callback);
    }
}
