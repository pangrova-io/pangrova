"""Regression checks for the source-based FQN review inventory."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("class_inventory", SCRIPTS / "export-class-inventory.py")
INVENTORY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INVENTORY)
POM = '<project xmlns="http://maven.apache.org/POM/4.0.0"><artifactId>test</artifactId></project>'


class ClassInventoryTest(unittest.TestCase):
    def test_worktree_tracks_new_and_removed_files_without_staging_and_reads_old_blob(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git = lambda *args: INVENTORY.git(root, *args)
            git("init", "-q")
            source = root / "example/src/main/java/example/Container.java"
            source.parent.mkdir(parents=True)
            (root / "example/pom.xml").write_text(POM)
            source.write_text("/* package wrong; */ package example; class Container { "
                              "record Entry(int n) {} void use() { class Local {} } } enum State { OK }")
            git("add", ".")
            git("-c", "user.name=Inventory Test", "-c", "user.email=test@example.org", "commit", "-qm", "base")
            after = source.with_name("Group.java")
            after.write_text("package example; class Group { enum State { IDLE } }")
            source.unlink()
            before = INVENTORY.declarations(INVENTORY.read_objects(root, "HEAD"))
            current = INVENTORY.declarations(INVENTORY.working_files(root))
            self.assertEqual(set(before), {"example.Container", "example.Container.Entry", "example.State"})
            self.assertEqual(set(current), {"example.Group", "example.Group.State"})
            rows = list(INVENTORY.rows(before, current))
            removed_state = next(row for row in rows if row[0] == "example.State")
            self.assertEqual(removed_state[3:8], ("", "", "", "removed FQN", "example.Group.State"))
            self.assertEqual(git("diff", "--cached", "--name-only"), b"")

    def test_rejects_duplicate_canonical_owners(self):
        body = "package fixture; class Shared {}"
        with self.assertRaisesRegex(ValueError, "Duplicate source owner for fixture.Shared"):
            INVENTORY.declarations({"a/pom.xml": POM, "b/pom.xml": POM,
                                    "a/src/main/java/fixture/A.java": body,
                                    "b/src/main/java/fixture/B.java": body})
