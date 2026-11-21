package org.pangrova.persistence.query.postgresql.transaction;

import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.postgres.SharedPostgres;
import org.pangrova.persistence.query.api.exception.JournalWriteException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcWriteCommitJournalInitializationTest extends ContainerTestBase {
    @Test
    void missingCanonicalSchemaFailsWithoutCreatingAParallelTable() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        assertThatThrownBy(() -> new JdbcWriteCommitJournal(source))
                .isInstanceOfSatisfying(JournalWriteException.class, failure -> {
                    assertThat(failure).hasMessageContaining("canonical schema");
                    assertThat(failure.admissionOutcome())
                            .isEqualTo(JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER);
                });
        try (var connection = source.getConnection(); var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT to_regclass('write_commit_journal')")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isNull();
        }
    }

    @Test
    void canonicalSchemaIsUsableAndMissingRequiredColumnsFailBeforeAdmission() throws Exception {
        var source = MigratedPostgres.create();
        var journal = new JdbcWriteCommitJournal(source);
        assertThat(journal.getEntry("absent")).isEmpty();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE write_commit_journal DROP COLUMN principal");
        }
        assertThatThrownBy(() -> new JdbcWriteCommitJournal(source))
                .isInstanceOf(JournalWriteException.class).hasMessageContaining("canonical schema");
    }
}
