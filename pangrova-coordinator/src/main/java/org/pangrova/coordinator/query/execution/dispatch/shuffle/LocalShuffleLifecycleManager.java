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
package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import org.pangrova.shuffle.ShuffleCommitResult;
import org.pangrova.shuffle.ShuffleHandle;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.shuffle.ShuffleRegistration;

/**
 * Local (in-process) shuffle lifecycle implementation used when no external
 * remote shuffle server is configured.
 *
 * <p>All operations return sentinel values ({@link ShuffleHandle#LOCAL},
 * {@link ShuffleCommitResult#EMPTY}) — there is no external state to manage.
 */
public final class LocalShuffleLifecycleManager implements ShuffleLifecycleManager {

    public static final LocalShuffleLifecycleManager INSTANCE = new LocalShuffleLifecycleManager();

    private LocalShuffleLifecycleManager() {}

    @Override
    public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
        return ShuffleHandle.LOCAL;
    }

    @Override
    public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
        return ShuffleCommitResult.EMPTY;
    }

    @Override
    public void cleanupShuffle(String queryId) {
        // Local shuffle has no external state to clean up.
    }
}
