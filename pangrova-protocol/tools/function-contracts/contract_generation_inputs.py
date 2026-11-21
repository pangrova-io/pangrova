"""Content identity of the actual local generator import closure."""

import ast
import hashlib
from pathlib import Path


def generation_input_files(entry):
    """Follow repository-local Python imports; standard-library code is not copied."""
    entry = Path(entry).resolve(strict=True)
    root = entry.parent
    pending, seen = [entry], set()
    while pending:
        path = pending.pop()
        if path in seen:
            continue
        if path.parent != root or len(seen) >= 256:
            raise ValueError("Function generator import closure escapes its bounded source root")
        seen.add(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.name)
        for node in ast.walk(tree):
            names = [alias.name for alias in node.names] if isinstance(node, ast.Import) else \
                [node.module] if isinstance(node, ast.ImportFrom) and node.module else []
            for name in names:
                candidate = root / (name.split(".")[0] + ".py")
                if candidate.is_file():
                    pending.append(candidate.resolve(strict=True))
    return tuple(sorted(seen, key=lambda path: path.name))


def generation_input_digest(declaration, entry=None):
    """Bind declaration bytes and executable generator sources, not only semantic JSON."""
    if entry is None:
        entry = Path(__file__).with_name("generate_execution_selection.py")
    inputs = [("declaration", Path(declaration).read_bytes())]
    inputs.extend(("generator/" + path.name, path.read_bytes()) for path in generation_input_files(entry))
    digest = hashlib.sha256(b"pangrova-function-generation-inputs-v1\0")
    for name, data in inputs:
        encoded = name.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()
