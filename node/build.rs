use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../ionian-client");

    let status = Command::new("go")
        .current_dir("../ionian-client")
        .args(vec!["build", "-o", "../target"])
        .status()
        .unwrap();

    println!("build ionian-client with status {}", status);
}
