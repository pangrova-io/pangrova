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
package org.pangrova.planner.core.analysis;

/**
 * Functional interface for re-optimizing a downstream sub-plan fragment using
 * actual runtime statistics from completed shuffle stages.
 *
 * <p>Implementations delegate to {@link PangrovaPlanner#reoptimize} to produce a
 * cost-optimized replacement fragment. If re-optimization fails or produces a
 * higher-cost plan, the original fragment is returned unchanged.</p>
 */
@FunctionalInterface
public interface SubPlanReoptimizer {

    /**
     * Re-optimizes the given plan fragment using runtime statistics.
     *
     * @param fragment the downstream sub-plan fragment to re-optimize
     * @return the re-optimized fragment, or the original if re-optimization
     *         does not produce a better plan
     */
    PlanFragment reoptimize(PlanFragment fragment);

    /**
     * Re-optimizes with an explicit join-strategy hint produced by AQE
     * (F-AQE-03). Default delegates to the no-hint overload; implementations
     * that consult the hint should override this method.
     */
    default PlanFragment reoptimize(PlanFragment fragment,
            org.pangrova.planner.api.adaptive.JoinStrategy joinStrategyHint) {
        return reoptimize(fragment);
    }
}
