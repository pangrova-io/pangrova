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
package org.pangrova.connector.iceberg;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Skips tests that require Hadoop when running on JDK 26+ where
 * {@code Subject.getSubject(AccessControlContext)} was removed (JEP 486).
 */
public final class HadoopTestGuard {

    private static final boolean HADOOP_COMPATIBLE = checkHadoopCompatibility();

    private HadoopTestGuard() {}

    public static void assumeHadoopCompatible() {
        assumeTrue(HADOOP_COMPATIBLE,
                "Hadoop requires JDK <= 25 (JEP 486 removed Subject.getSubject)");
    }

    public static boolean isHadoopCompatible() {
        return HADOOP_COMPATIBLE;
    }

    @SuppressWarnings({"removal", "deprecation"})
    private static boolean checkHadoopCompatibility() {
        try {
            javax.security.auth.Subject.getSubject(null);
            return true;
        } catch (UnsupportedOperationException e) {
            return false;
        } catch (NullPointerException e) {
            return true;
        }
    }
}
