"""Actual generator closure identity is distinct from declaration semantics."""

from pathlib import Path
import tempfile
import unittest

from contract_generation_inputs import generation_input_digest, generation_input_files


DECLARATION = Path(__file__).resolve().parents[2] / "src/main/resources/function-contracts/execution-selection.json"


class ContractGenerationInputsTest(unittest.TestCase):
    def test_actual_closure_includes_the_generated_special_programme_and_formula_sources(self):
        entry = Path(__file__).with_name("generate_execution_selection.py")
        names = {path.name for path in generation_input_files(entry)}
        self.assertTrue({"generate_execution_selection.py", "execution_selection.py",
                         "generate_special_type_programs.py", "special_type_programs.py",
                         "type_formula.py", "registered_signature.py",
                         "contract_generation_inputs.py"}.issubset(names))
        self.assertEqual(generation_input_digest(DECLARATION), generation_input_digest(DECLARATION))

    def test_transitive_source_change_is_not_hidden_by_identical_declaration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            entry, imported, declaration = root / "entry.py", root / "child.py", root / "contract.json"
            entry.write_text("import child\n", encoding="utf-8")
            imported.write_text("value = 1\n", encoding="utf-8")
            declaration.write_text("{}\n", encoding="utf-8")
            before = generation_input_digest(declaration, entry)
            imported.write_text("value = 2\n", encoding="utf-8")
            self.assertNotEqual(before, generation_input_digest(declaration, entry))

    def test_local_import_cannot_escape_the_owned_generator_source_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owned = root / "owned"
            owned.mkdir()
            outside = root / "outside.py"
            outside.write_text("value = 1\n", encoding="utf-8")
            entry = owned / "entry.py"
            entry.write_text("import child\n", encoding="utf-8")
            (owned / "child.py").symlink_to(outside)
            with self.assertRaisesRegex(ValueError, "escapes"):
                generation_input_files(entry)


if __name__ == "__main__":
    unittest.main()
