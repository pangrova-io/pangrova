#!/usr/bin/env python3
"""Build a real, immutable Ollama fixture before tests; tests never download models."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


VERSION = "0.6.2"
INFERENCE_THREADS = 2
MODELS = {"tinyllama:latest": "completion-digest", "all-minilm:latest": "embedding-digest"}
LABEL_PREFIX = "org.pangrova.ai-fixture."
PARAMETER_LAYER = "application/vnd.ollama.image.params"


def docker(*args, input_text=None):
    return subprocess.check_output(
        ["docker", *args], text=True, input=input_text, timeout=600).strip()


def request(endpoint, path, body=None, timeout=30):
    payload = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(endpoint + path, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        value = json.load(response)
    if value.get("error"):
        raise ValueError(f"Ollama {path} failed: {value['error']}")
    return value


def download_models(base, destination, dns):
    name = "pangrova-ai-fixture-" + uuid.uuid4().hex
    args = ["create", "--name", name, "--publish", "127.0.0.1::11434"]
    for server in dns:
        args.extend(["--dns", server])
    docker(*args, base, "serve")
    try:
        docker("start", name)
        port = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Ports"]["11434/tcp"][0]["HostPort"]
        endpoint = "http://127.0.0.1:" + port
        deadline = time.monotonic() + 30
        while True:
            try:
                actual_version = request(endpoint, "/api/version", timeout=2)["version"]
                break
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        if actual_version != VERSION:
            raise ValueError(f"Expected Ollama {VERSION}, found {actual_version}")
        for model in MODELS:
            result = request(endpoint, "/api/pull", {"model": model, "stream": False}, timeout=600)
            if result.get("status") != "success":
                raise ValueError(f"Model preparation did not complete: {model}: {result}")
        docker("stop", name)
        docker("cp", name + ":/root/.ollama/models", str(destination))
    finally:
        docker("rm", "--force", "--volumes", name)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def model_manifest(directory, model):
    name, tag = model.split(":")
    return directory / "manifests" / "registry.ollama.ai" / "library" / name / tag


def verify_models(directory):
    """Verify real manifest bytes, every referenced blob and sizes before image creation."""
    digests = {}
    verified_blobs = set()
    for model, label in MODELS.items():
        manifest = model_manifest(directory, model)
        content = json.loads(manifest.read_bytes())
        for entry in [content["config"], *content["layers"]]:
            digest = entry["digest"]
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise ValueError(f"Invalid blob identity for {model}: {digest}")
            blob = directory / "blobs" / digest.replace(":", "-")
            if blob.stat().st_size != entry["size"]:
                raise ValueError(f"Blob size differs from manifest: {blob.name}")
            if digest not in verified_blobs:
                if sha256(blob) != digest.removeprefix("sha256:"):
                    raise ValueError(f"Blob checksum differs from manifest: {blob.name}")
                verified_blobs.add(digest)
        digests[label] = sha256(manifest)
    return digests


def configure_models(container):
    deadline = time.monotonic() + 30
    while True:
        probe = subprocess.run(["docker", "exec", container, "ollama", "list"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               timeout=5, check=False)
        if probe.returncode == 0:
            break
        if time.monotonic() >= deadline:
            raise ValueError("Offline Ollama failed to start for model configuration")
        time.sleep(0.1)
    for model in MODELS:
        modelfile = f"FROM {model}\nPARAMETER num_thread {INFERENCE_THREADS}\n"
        docker("exec", "-i", container, "ollama", "create", model, "-f", "/dev/stdin", input_text=modelfile)


def non_parameter_layers(manifest):
    return [(layer["mediaType"], layer["digest"]) for layer in manifest["layers"]
            if layer["mediaType"] != PARAMETER_LAYER]


def verify_configured_models(original, configured):
    identities = verify_models(configured)
    for model in MODELS:
        source = json.loads(model_manifest(original, model).read_bytes())
        deployed = json.loads(model_manifest(configured, model).read_bytes())
        if non_parameter_layers(source) != non_parameter_layers(deployed):
            raise ValueError(f"Configuring the fixture changed a non-parameter layer for {model}")
        parameters = [layer for layer in deployed["layers"] if layer["mediaType"] == PARAMETER_LAYER]
        if len(parameters) != 1:
            raise ValueError(f"Expected one parameter layer for {model}")
        parameter_blob = configured / "blobs" / parameters[0]["digest"].replace(":", "-")
        if json.loads(parameter_blob.read_bytes()).get("num_thread") != INFERENCE_THREADS:
            raise ValueError(f"Ollama did not record the expected inference thread count for {model}")
    return identities


def freeze_image(base, models):
    # Create directly from the exact local image ID. BuildKit treats FROM sha256:...
    # as a registry reference, which would reintroduce a network dependency.
    name = "pangrova-ai-image-" + uuid.uuid4().hex
    docker("create", "--name", name, "--network", "none", base, "serve")
    try:
        bundle = models.parent / ".ollama"
        bundle.mkdir()
        models.rename(bundle / "models")
        docker("cp", str(bundle), name + ":/root/")
        docker("start", name)
        configure_models(name)
        docker("stop", name)
        configured = models.parent / "configured-models"
        docker("cp", name + ":/root/.ollama/models", str(configured))
        identities = verify_configured_models(bundle / "models", configured)
        labels = {"base-image": base, "ollama-version": VERSION,
                  "inference-threads": str(INFERENCE_THREADS), **identities}
        changes = []
        for key, value in labels.items():
            changes.extend(["--change", f"LABEL {LABEL_PREFIX}{key}={json.dumps(value)}"])
        return docker("commit", *changes, name), labels
    finally:
        docker("rm", "--force", "--volumes", name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--models-dir", type=Path, help="Previously downloaded real Ollama models; all bytes are verified")
    parser.add_argument("--dns", action="append", default=[], help="DNS for the download container only")
    args = parser.parse_args()
    if args.receipt.exists():
        parser.error("Receipt already exists; choose a new output file")
    base = json.loads(docker("image", "inspect", "ollama/ollama:" + VERSION))[0]
    if base["Config"].get("Volumes"):
        raise ValueError("The fixture base must retain baked models in its filesystem")
    with tempfile.TemporaryDirectory(prefix="pangrova-ai-fixture-") as temporary:
        root = Path(temporary)
        models = root / "models"
        if args.models_dir is None:
            download_models(base["Id"], models, args.dns)
        else:
            shutil.copytree(args.models_dir, models)
        verify_models(models)
        image, labels = freeze_image(base["Id"], models)
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
            raise ValueError("Docker did not produce an immutable fixture image ID")
        actual = json.loads(docker("image", "inspect", image))[0]
        for key, value in labels.items():
            if actual["Config"]["Labels"].get(LABEL_PREFIX + key) != value:
                raise ValueError(f"Built image lost its {key} receipt")
        receipt = {"imageId": image, "architecture": actual["Architecture"], **labels}
        with args.receipt.open("x") as output:
            json.dump(receipt, output, indent=2)
            output.write("\n")
        print(image)


if __name__ == "__main__":
    main()
