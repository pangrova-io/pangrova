use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let crate_root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest root"));
    let protocol = crate_root.join("../../pangrova-protocol/src/main/proto");
    let imports = crate_root.join("../../pangrova-protocol/src/main/proto-imports");
    let directory = protocol.join("pangrova/protocol/v2");
    let mut inputs: Vec<PathBuf> = fs::read_dir(&directory)
        .expect("Canonical Pangrova v2 sources are required; mirrored Rust protocols are forbidden")
        .map(|entry| entry.expect("Canonical protocol directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "proto")
        })
        .collect();
    inputs.sort();
    assert!(
        !inputs.is_empty(),
        "Canonical Pangrova v2 protocol tree is empty"
    );
    println!("cargo:rerun-if-changed={}", protocol.display());
    println!("cargo:rerun-if-changed={}", imports.display());
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=PROTOC_INCLUDE");
    let mut config = prost_build::Config::new();
    config.include_file("canonical.rs");
    // Deterministic map ordering preserves Java/C++ canonical provenance hashes.
    config.btree_map(["."]);
    config.file_descriptor_set_path(
        PathBuf::from(env::var_os("OUT_DIR").expect("Cargo output directory"))
            .join("canonical_v2.descriptor.bin"),
    );
    config
        .compile_protos(&inputs, &[protocol, imports])
        .expect("Generate Rust messages from the sole canonical Pangrova v2 protocol tree");
}
