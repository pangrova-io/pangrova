package org.pangrova.parser.syntax;


import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;
import org.pangrova.parser.api.ast.statement.AlterStageAction;
import org.pangrova.parser.api.ast.statement.AlterStageStatement;
import org.pangrova.parser.api.ast.statement.AlterVolumeAction;
import org.pangrova.parser.api.ast.statement.AlterVolumeStatement;
import org.pangrova.parser.api.ast.statement.CreateStageStatement;
import org.pangrova.parser.api.ast.statement.CreateVolumeStatement;
import org.pangrova.parser.api.ast.statement.DescribeStageStatement;
import org.pangrova.parser.api.ast.statement.DescribeVolumeStatement;
import org.pangrova.parser.api.ast.statement.DropStageStatement;
import org.pangrova.parser.api.ast.statement.DropVolumeStatement;
import org.pangrova.parser.api.ast.statement.ListStageStatement;
import org.pangrova.parser.api.ast.statement.RemoveStageStatement;
import org.pangrova.parser.api.ast.statement.ShowStagesStatement;
import org.pangrova.parser.api.ast.statement.ShowVolumesStatement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Parser coverage for the Volume and Stage DDL surfaces.
 */
class FileSpaceDdlParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Nested
    class CreateVolumeTests {

        @Test
        void managedVolumeDefaults() {
            Statement stmt = parser.parse("CREATE VOLUME media.raw.landing");

            assertThat(stmt).isInstanceOf(CreateVolumeStatement.class);
            CreateVolumeStatement volume = (CreateVolumeStatement) stmt;
            assertThat(volume.name().parts()).containsExactly("media", "raw", "landing");
            assertThat(volume.external()).isFalse();
            assertThat(volume.ifNotExists()).isFalse();
            assertThat(volume.location()).isEmpty();
            assertThat(volume.storageProfile()).isEmpty();
            assertThat(volume.comment()).isEmpty();
            assertThat(stmt.type()).isEqualTo(StatementType.CREATE_VOLUME);
        }

        @Test
        void explicitManagedVolume() {
            CreateVolumeStatement volume =
                    (CreateVolumeStatement) parser.parse("CREATE MANAGED VOLUME v");

            assertThat(volume.external()).isFalse();
        }

        @Test
        void externalVolumeWithOptions() {
            CreateVolumeStatement volume = (CreateVolumeStatement) parser.parse(
                    "CREATE EXTERNAL VOLUME IF NOT EXISTS media.raw.landing "
                            + "LOCATION 's3://acme/landing/' "
                            + "STORAGE_PROFILE 'prod-s3' COMMENT 'landing zone'");

            assertThat(volume.external()).isTrue();
            assertThat(volume.ifNotExists()).isTrue();
            assertThat(volume.location()).contains("s3://acme/landing/");
            assertThat(volume.storageProfile()).contains("prod-s3");
            assertThat(volume.comment()).contains("landing zone");
        }

        @Test
        void acceptsTwoWordStorageProfileSpelling() {
            CreateVolumeStatement volume =
                    (CreateVolumeStatement) parser.parse("CREATE VOLUME v STORAGE PROFILE 'p'");

            assertThat(volume.storageProfile()).contains("p");
        }
    }

    @Nested
    class AlterVolumeTests {

        @Test
        void setStorageProfile() {
            AlterVolumeStatement stmt =
                    (AlterVolumeStatement) parser.parse("ALTER VOLUME media.raw.landing "
                            + "SET STORAGE_PROFILE 'prod-s3-v2'");

            assertThat(stmt.action())
                    .isEqualTo(new AlterVolumeAction.SetStorageProfile("prod-s3-v2"));
        }

        @Test
        void setComment() {
            AlterVolumeStatement stmt = (AlterVolumeStatement) parser.parse(
                    "ALTER VOLUME media.raw.landing SET COMMENT 'updated'");

            assertThat(stmt.action()).isEqualTo(new AlterVolumeAction.SetComment("updated"));
        }

        @Test
        void setOwner() {
            AlterVolumeStatement stmt = (AlterVolumeStatement) parser.parse(
                    "ALTER VOLUME media.raw.landing SET OWNER TO alice");

            assertThat(stmt.action()).isEqualTo(new AlterVolumeAction.SetOwner("alice"));
        }

        @Test
        void rename() {
            AlterVolumeStatement stmt = (AlterVolumeStatement) parser.parse(
                    "ALTER VOLUME media.raw.landing RENAME TO media.raw.landing_v2");

            assertThat(stmt.name().parts()).containsExactly("media", "raw", "landing");
            assertThat(stmt.action())
                    .isEqualTo(new AlterVolumeAction.Rename(
                            org.pangrova.parser.api.ast.Identifier.of(
                                    "media", "raw", "landing_v2")));
        }
    }

    @Nested
    class VolumeMetadataTests {

        @Test
        void dropVolume() {
            DropVolumeStatement stmt = (DropVolumeStatement) parser.parse(
                    "DROP VOLUME IF EXISTS media.raw.landing");

            assertThat(stmt.name().parts()).containsExactly("media", "raw", "landing");
            assertThat(stmt.ifExists()).isTrue();
            assertThat(stmt.type()).isEqualTo(StatementType.DROP_VOLUME);
        }

        @Test
        void showVolumesWithoutScope() {
            ShowVolumesStatement stmt = (ShowVolumesStatement) parser.parse("SHOW VOLUMES");

            assertThat(stmt.qualifiedName()).isEmpty();
            assertThat(stmt.type()).isEqualTo(StatementType.SHOW_VOLUMES);
        }

        @Test
        void showVolumesInSchema() {
            ShowVolumesStatement stmt =
                    (ShowVolumesStatement) parser.parse("SHOW VOLUMES FROM media.raw");

            assertThat(stmt.qualifiedName()).containsExactly("media", "raw");
        }

        @Test
        void describeVolume() {
            DescribeVolumeStatement stmt = (DescribeVolumeStatement) parser.parse(
                    "DESCRIBE VOLUME media.raw.landing");

            assertThat(stmt.name().parts()).containsExactly("media", "raw", "landing");
            assertThat(stmt.type()).isEqualTo(StatementType.DESCRIBE_VOLUME);
        }
    }

    @Nested
    class CreateStageTests {

        @Test
        void internalStageDefaults() {
            CreateStageStatement stage =
                    (CreateStageStatement) parser.parse("CREATE STAGE media.raw.stage_landing");

            assertThat(stage.external()).isFalse();
            assertThat(stage.ifNotExists()).isFalse();
            assertThat(stage.url()).isEmpty();
            assertThat(stage.storageProfile()).isEmpty();
            assertThat(stage.directoryEnabled()).isFalse();
            assertThat(stage.comment()).isEmpty();
            assertThat(stage.type()).isEqualTo(StatementType.CREATE_STAGE);
        }

        @Test
        void externalStageWithDirectory() {
            CreateStageStatement stage = (CreateStageStatement) parser.parse(
                    "CREATE EXTERNAL STAGE IF NOT EXISTS media.raw.stage_landing "
                            + "URL = 's3://acme/stage/' "
                            + "STORAGE_PROFILE = 'prod-s3' "
                            + "DIRECTORY = (ENABLE = TRUE) COMMENT = 'landing'");

            assertThat(stage.external()).isTrue();
            assertThat(stage.ifNotExists()).isTrue();
            assertThat(stage.url()).contains("s3://acme/stage/");
            assertThat(stage.storageProfile()).contains("prod-s3");
            assertThat(stage.directoryEnabled()).isTrue();
            assertThat(stage.comment()).contains("landing");
        }
    }

    @Nested
    class AlterStageTests {

        @Test
        void refreshWholeStage() {
            AlterStageStatement stmt = (AlterStageStatement) parser.parse(
                    "ALTER STAGE media.raw.stage_landing REFRESH");

            assertThat(stmt.action()).isEqualTo(new AlterStageAction.Refresh(
                    java.util.Optional.empty()));
        }

        @Test
        void refreshSubpath() {
            AlterStageStatement stmt = (AlterStageStatement) parser.parse(
                    "ALTER STAGE media.raw.stage_landing REFRESH SUBPATH = 'images/'");

            assertThat(stmt.action()).isEqualTo(new AlterStageAction.Refresh(
                    java.util.Optional.of("images/")));
        }

        @Test
        void setUrl() {
            AlterStageStatement stmt = (AlterStageStatement) parser.parse(
                    "ALTER STAGE media.raw.stage_landing SET URL = 's3://acme/stage-v2/'");

            assertThat(stmt.action())
                    .isEqualTo(new AlterStageAction.SetUrl("s3://acme/stage-v2/"));
        }

        @Test
        void toggleDirectory() {
            AlterStageStatement stmt = (AlterStageStatement) parser.parse(
                    "ALTER STAGE media.raw.stage_landing "
                            + "SET DIRECTORY = (ENABLE = FALSE)");

            assertThat(stmt.action())
                    .isEqualTo(new AlterStageAction.SetDirectoryEnabled(false));
        }

        @Test
        void setOwner() {
            AlterStageStatement stmt = (AlterStageStatement) parser.parse(
                    "ALTER STAGE media.raw.stage_landing SET OWNER TO alice");

            assertThat(stmt.action()).isEqualTo(new AlterStageAction.SetOwner("alice"));
        }

        @Test
        void rename() {
            AlterStageStatement stmt = (AlterStageStatement) parser.parse(
                    "ALTER STAGE media.raw.stage_landing RENAME TO media.raw.stage_v2");

            assertThat(stmt.action()).isInstanceOf(AlterStageAction.Rename.class);
        }
    }

    @Nested
    class StageMetadataTests {

        @Test
        void dropStage() {
            DropStageStatement stmt =
                    (DropStageStatement) parser.parse("DROP STAGE IF EXISTS media.raw.stage_landing");

            assertThat(stmt.name().parts()).containsExactly("media", "raw", "stage_landing");
            assertThat(stmt.ifExists()).isTrue();
            assertThat(stmt.type()).isEqualTo(StatementType.DROP_STAGE);
        }

        @Test
        void showStages() {
            ShowStagesStatement all = (ShowStagesStatement) parser.parse("SHOW STAGES");
            ShowStagesStatement scoped =
                    (ShowStagesStatement) parser.parse("SHOW STAGES IN media.raw");

            assertThat(all.qualifiedName()).isEmpty();
            assertThat(scoped.qualifiedName()).containsExactly("media", "raw");
        }

        @Test
        void describeStage() {
            DescribeStageStatement stmt = (DescribeStageStatement) parser.parse(
                    "DESCRIBE STAGE media.raw.stage_landing");

            assertThat(stmt.name().parts()).containsExactly("media", "raw", "stage_landing");
            assertThat(stmt.type()).isEqualTo(StatementType.DESCRIBE_STAGE);
        }
    }

    @Nested
    class ListStageTests {

        @Test
        void listStageRoot() {
            ListStageStatement stmt =
                    (ListStageStatement) parser.parse("LIST @media.raw.stage_landing");

            assertThat(stmt.name().parts()).containsExactly("media", "raw", "stage_landing");
            assertThat(stmt.path()).isEmpty();
            assertThat(stmt.pattern()).isEmpty();
            assertThat(stmt.type()).isEqualTo(StatementType.LIST_STAGE);
        }

        @Test
        void listStagePrefixWithoutTrailingSlash() {
            ListStageStatement stmt = (ListStageStatement) parser.parse(
                    "LIST @media.raw.stage_landing/images/2026/10");

            assertThat(stmt.path()).contains("images/2026/10");
        }

        @Test
        void listStagePrefixKeepsFileExtension() {
            ListStageStatement stmt =
                    (ListStageStatement) parser.parse("LIST @stage/images/cat.jpg");

            assertThat(stmt.name().parts()).containsExactly("stage");
            assertThat(stmt.path()).contains("images/cat.jpg");
        }

        @Test
        void listStageWithPatternAndTrailingSlash() {
            ListStageStatement stmt = (ListStageStatement) parser.parse(
                    "LIST @media.raw.stage_landing/images/ PATTERN = '.*[.]jpg'");

            assertThat(stmt.path()).contains("images");
            assertThat(stmt.pattern()).contains(".*[.]jpg");
        }
    }

    @Nested
    class RemoveStageTests {

        @Test
        void removeStageRoot() {
            RemoveStageStatement stmt =
                    (RemoveStageStatement) parser.parse("REMOVE @media.raw.stage_landing");

            assertThat(stmt.name().parts()).containsExactly("media", "raw", "stage_landing");
            assertThat(stmt.path()).isEmpty();
            assertThat(stmt.pattern()).isEmpty();
            assertThat(stmt.type()).isEqualTo(StatementType.REMOVE_STAGE);
        }

        @Test
        void removeStagePathWithPattern() {
            RemoveStageStatement stmt = (RemoveStageStatement) parser.parse(
                    "REMOVE @media.raw.stage_landing/images/ PATTERN = '.*[.]jpg'");

            assertThat(stmt.path()).contains("images");
            assertThat(stmt.pattern()).contains(".*[.]jpg");
        }

        @Test
        void removeStagePrefixKeepsNumericSegments() {
            RemoveStageStatement stmt = (RemoveStageStatement) parser.parse(
                    "REMOVE @stage/images/2026/10/");

            assertThat(stmt.path()).contains("images/2026/10");
        }
    }

    @Test
    void volumeAndStageKeywordsRemainUsableAsIdentifiers() {
        Statement stmt = parser.parse(
                "CREATE TABLE t (volume INT, stage INT, url VARCHAR, list INT, "
                        + "remove INT)");
        Statement named = parser.parse("CREATE TABLE stage (volume INT)");

        assertThat(stmt.type()).isEqualTo(StatementType.CREATE_TABLE);
        assertThat(named.type()).isEqualTo(StatementType.CREATE_TABLE);
    }
}
