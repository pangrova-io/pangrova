"""Regression checks for POM discovery, shared AST source facts and fail-closed gates."""
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("module_baseline", SCRIPTS / "generate-module-baseline.py")
BASELINE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASELINE)


class ModuleManifestTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.original_root = BASELINE.ROOT
        BASELINE.ROOT = self.root

    def tearDown(self):
        BASELINE.ROOT = self.original_root
        self.temp.cleanup()

    def test_discovery_prunes_build_artifacts_but_finds_undeclared_source_poms(self):
        production = self.root / "new-family" / "undeclared-leaf"
        production.mkdir(parents=True)
        (production / "pom.xml").write_text(
            '<project xmlns="http://maven.apache.org/POM/4.0.0">'
            '<artifactId>new-owner</artifactId></project>'
        )
        for ignored in ("target", ".git", "node_modules"):
            directory = self.root / ignored / "stale"
            directory.mkdir(parents=True)
            (directory / "pom.xml").write_text("not valid XML")
        self.assertEqual({"new-owner"}, set(BASELINE.pom_artifacts()))

    def test_resource_jar_is_an_artifact_even_without_java_sources(self):
        (self.root / "pom.xml").write_text(
            '<project xmlns="http://maven.apache.org/POM/4.0.0">'
            '<artifactId>schema-resources</artifactId><packaging>jar</packaging></project>'
        )
        resource = self.root / "src/main/resources/db/migration/V1__initial.sql"
        resource.parent.mkdir(parents=True)
        resource.write_text("SELECT 1;")
        facts = BASELINE.pom_artifacts()["schema-resources"]
        self.assertTrue(facts["leaf"])
        self.assertEqual("jar", facts["packaging"])

    def test_graphs_include_resource_modules_and_keep_pom_scopes_separate_from_package_ownership(self):
        leaves = {"app": {"dependencies": {"runtime": "runtime", "fixtures": "test"}},
                  "runtime": {"dependencies": {}}, "fixtures": {"dependencies": {}}}
        packages = {"app": {"org.example.app"}, "runtime": set(), "fixtures": set()}
        BASELINE.export_graphs(self.root, leaves, packages)
        dependencies = (self.root / "module-dependencies.dot").read_text()
        self.assertIn('"app" -> "runtime" [label="runtime"];', dependencies)
        self.assertIn('"app" -> "fixtures" [label="test"];', dependencies)
        ownership = (self.root / "package-ownership.dot").read_text()
        self.assertIn('"runtime" [shape=box];', ownership)
        self.assertIn('"app" -> "org.example.app";', ownership)
        self.assertNotIn('"app" -> "fixtures"', ownership)

    def test_merging_leaf_artifacts_reports_external_path_back_to_another_selected_leaf(self):
        deps = {"core": ["api", "provider"], "provider": ["port"], "port": ["api"], "api": []}
        artifacts = {name: {"leaf": True, "dependencies": dict.fromkeys(edges, "compile")}
                     for name, edges in deps.items()}
        self.assertEqual([("core", "provider"), ("provider", "port"), ("port", "api")],
                         BASELINE.merge_witness(artifacts, [["core", "api"]]))
        self.assertEqual([], BASELINE.merge_witness(artifacts, [["core", "provider"]]))
        self.assertEqual([], BASELINE.merge_witness(artifacts, [list(deps)]))
        with self.assertRaisesRegex(ValueError, "Unknown or non-leaf"):
            BASELINE.merge_witness(artifacts, [["api", "missing"]])

    def test_two_individually_safe_merges_can_close_a_cycle_together(self):
        deps = {"a-app": ["b-api"], "b-app": ["a-api"], "a-api": [], "b-api": []}
        artifacts = {name: {"leaf": True, "dependencies": dict.fromkeys(edges, "compile")}
                     for name, edges in deps.items()}
        groups = [["a-app", "a-api"], ["b-app", "b-api"]]
        for group in groups:
            self.assertEqual([], BASELINE.merge_witness(artifacts, [group]))
        self.assertEqual([("a-app", "b-api"), ("b-app", "a-api")],
                         BASELINE.merge_witness(artifacts, groups))
        with self.assertRaisesRegex(ValueError, "multiple merge groups: a-api"):
            BASELINE.merge_witness(artifacts, [groups[0], ["a-api", "b-api"]])

    def test_duplicate_artifact_cannot_hide_a_module_from_the_graph(self):
        for directory in ("first", "second"):
            module = self.root / directory
            module.mkdir()
            (module / "pom.xml").write_text(
                '<project xmlns="http://maven.apache.org/POM/4.0.0">'
                '<artifactId>same-owner</artifactId></project>')
        with self.assertRaisesRegex(ValueError, "same-owner: first/pom.xml and second/pom.xml"):
            BASELINE.pom_artifacts()

    def test_ambiguous_package_cannot_redirect_an_import_to_the_last_module(self):
        for name in ("first", "second"):
            module = self.root / name
            source = module / "src/main/java/org/pangrova/fixture"
            source.mkdir(parents=True)
            (module / "pom.xml").write_text(
                f'<project xmlns="http://maven.apache.org/POM/4.0.0"><artifactId>{name}</artifactId></project>')
            (source / f"{name.title()}.java").write_text(
                f"package org.pangrova.fixture; public class {name.title()} {{}}")
        with self.assertRaisesRegex(ValueError, "Split package org.pangrova.fixture: first and second"):
            BASELINE.main()

    def test_single_child_family_must_be_flattened(self):
        family = self.root / "family"
        family.mkdir()
        (family / "pom.xml").write_text(
            '<project xmlns="http://maven.apache.org/POM/4.0.0"><artifactId>family</artifactId>'
            '<packaging>pom</packaging><modules><module>only-child</module></modules></project>')
        with self.assertRaisesRegex(ValueError, "family/pom.xml aggregates only only-child"):
            BASELINE.pom_artifacts()

    def test_invalid_source_pom_is_not_ignored(self):
        (self.root / "pom.xml").write_text("not valid XML")
        with self.assertRaises(BASELINE.ET.ParseError):
            BASELINE.pom_artifacts()

    def test_missing_source_tree_is_not_an_empty_success(self):
        BASELINE.ROOT = self.root / "missing"
        with self.assertRaises(FileNotFoundError):
            BASELINE.pom_artifacts()

    def test_discovery_failure_cannot_be_reported_as_success(self):
        scripts = self.root / ".pangrova" / "scripts"
        scripts.mkdir(parents=True)
        gate = scripts / "check-module-manifest.sh"
        gate.write_text((SCRIPTS / gate.name).read_text())
        (self.root / ".pangrova" / "modules.yaml").write_text("artifacts: {}\n")
        generator = scripts / "generate-module-baseline.py"
        generator.write_text("#!/bin/sh\nexit 0\n")
        generator.chmod(0o755)
        fake_bin = self.root / "bin"
        fake_bin.mkdir()
        interpreter = fake_bin / "python3"
        interpreter.write_text("#!/bin/sh\necho discovery_failed >&2\nexit 23\n")
        interpreter.chmod(0o755)
        env = dict(os.environ, PATH=str(fake_bin) + os.pathsep + os.environ["PATH"])
        result = subprocess.run(["bash", str(gate)], env=env, text=True, capture_output=True)
        self.assertEqual(23, result.returncode, result.stdout + result.stderr)
        self.assertIn("discovery_failed", result.stderr)
        self.assertNotIn("OK:", result.stdout)

    def test_source_launcher_collects_the_same_ast_facts_for_main_and_test(self):
        main = self.root / "owner with spaces" / "src/main/java"
        test = self.root / "owner with spaces" / "src/test/java"
        (main / "org/pangrova/owner").mkdir(parents=True)
        (test / "org/pangrova/owner").mkdir(parents=True)
        (main / "org/pangrova/owner/Use.java").write_text(
            "/* package org.pangrova.fake; */\npackage org.pangrova.owner;\n"
            + r"imp\u006frt org.pangrova.producer.*;" + "\n"
            + "import static org.pangrova.other.Host.*;\n"
            + "public class Use { Object value = new org.pangrova.internal.Hidden();\n"
            + 'String example = """\nimport org.pangrova.fake.Fake;\n"""; }'
        )
        (test / "org/pangrova/owner/UseTest.java").write_text(
            "package org.pangrova.owner;\npublic class UseTest {"
            "Object value = org.pangrova.fixture.Factory.create().toString(); }"
        )
        facts = BASELINE.source_facts([main, test, self.root / "absent-source-set"])
        self.assertEqual({"org.pangrova.owner"}, facts[0]["packages"])
        self.assertEqual({"org.pangrova.producer.*", "org.pangrova.other.Host.*",
                          "org.pangrova.internal.Hidden"}, facts[0]["references"])
        self.assertEqual({"org.pangrova.fixture.Factory.create"}, facts[1]["references"])
        self.assertEqual({"packages": set(), "references": set()}, facts[2])

    def test_invalid_java_fails_with_source_diagnostics(self):
        source = self.root / "source"
        source.mkdir()
        (source / "org/pangrova/broken").mkdir(parents=True)
        (source / "org/pangrova/broken/Broken.java").write_text("package org.pangrova.broken; class Broken { invalid ! }")
        with self.assertRaisesRegex(RuntimeError, "Java source scan failed") as failure:
            BASELINE.source_facts([source])
        self.assertIn("Cannot parse architecture sources", str(failure.exception))
        self.assertIn("Broken.java", str(failure.exception))

    def test_invalid_utf8_is_not_ignored(self):
        source = self.root / "source"
        source.mkdir()
        (source / "Broken.java").write_bytes(b'package org.pangrova.broken; class Broken { String s="\xff"; }')
        with self.assertRaisesRegex(RuntimeError, "Java source scan failed"):
            BASELINE.source_facts([source])


if __name__ == "__main__":
    unittest.main()
