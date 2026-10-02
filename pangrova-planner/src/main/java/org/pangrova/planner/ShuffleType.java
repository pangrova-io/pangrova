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
package org.pangrova.planner;

/**
 * Distribution type for a shuffle exchange between pipeline stages.
 * Coordinator-friendly equivalent of Calcite's {@code RelDistribution.Type},
 * keeping Calcite internals hidden within the planner module.
 */
public enum ShuffleType {
    BROADCAST,
    HASH,
    RANGE,
    ROUND_ROBIN,
    SINGLETON,
    ANY
}
