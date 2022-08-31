use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../ionian-client");

    let _ = Command::new("git")
        .args(&["submodule", "update", "--init"])
        .status();

    let status = Command::new("go")
        .current_dir("../ionian-client")
        .args(vec!["build", "-o", "../target"])
        .status()
        .unwrap();

    println!("build ionian-client with status {}", status);
}
