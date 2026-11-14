package org.pangrova.testing.integration.e2e.property.write;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Tag;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.testing.integration.e2e.write.NativeWriteLifecycleFixture;
import org.pangrova.testing.integration.e2e.write.NativeWriteLifecycleTestBase;
import org.pangrova.worker.exception.NativeRuntimeException;

import java.nio.file.Path;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Every actual SDK writer in the generated cohort must release its files and reject further appends. */
@Tag("integration")
@Tag("native-required")
class NativeWriteExecutorSessionCleanupPropertyTest extends NativeWriteLifecycleTestBase {
    @Property(tries = 100)
    void closeAllSessionsDrainsAllActiveSessions(
            @ForAll @IntRange(min = 1, max = 100) int sessionCount) throws Exception {
        String table = newTable();
        try (var fixture = lifecycle()) {
            var sessions = new ArrayList<NativeWriteLifecycleFixture.OpenSession>();
            var files = new ArrayList<Path>();
            for (int i = 0; i < sessionCount; i++) {
                var opened = fixture.open(table);
                sessions.add(opened);
                files.add(fixture.appendAndFindOwnedFile(opened, i));
            }
            assertThat(fixture.executor().activeSessionCount()).isEqualTo(sessionCount);
            assertThat(files).hasSize(sessionCount).doesNotHaveDuplicates();
            fixture.executor().closeAllSessions();
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(files).allSatisfy(path -> assertThat(path).doesNotExist());
            assertThat(NativeWriteLifecycleFixture.files(sessions.get(0))).isEmpty();
            for (var opened : sessions) {
                assertThatThrownBy(() -> fixture.append(opened.handle(), 1))
                        .isInstanceOf(NativeRuntimeException.class).hasMessageContaining("No active write session");
            }
        }
    }
}
