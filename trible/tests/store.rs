use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn store_blob_list_outputs_file() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("file.bin");
    let contents = b"hi";
    std::fs::write(&file, contents).unwrap();

    let url = format!("file://{}", dir.path().display());

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\n$");

    // Upload via CLI and ensure put prints the handle
    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match(&pattern).unwrap());

    // Now list should show the repo-managed blob handle
    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "list", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains(&digest));
}

#[test]
fn store_blob_put_uploads_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("input.bin");
    let contents = b"hi there";
    std::fs::write(&file_path, contents).unwrap();

    let url = format!("file://{}", dir.path().display());

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\\n$");

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, file_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match(&pattern).unwrap());

    let blob_path = dir.path().join("blobs").join(digest);
    assert!(blob_path.exists());
}

#[test]
fn store_blob_forget_removes_blob() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("input.bin");
    let contents = b"remove me";
    std::fs::write(&file_path, contents).unwrap();

    let url = format!("file://{}", dir.path().display());

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\\n$");

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, file_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match(&pattern).unwrap());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "forget", &url, &handle])
        .assert()
        .success();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "list", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains(&digest).not());
}

#[test]
fn store_blob_get_downloads_file() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("input.bin");
    let output_path = dir.path().join("output.bin");
    let contents = b"remote blob";
    std::fs::write(&input_path, contents).unwrap();

    let url = format!("file://{}", dir.path().display());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, input_path.to_str().unwrap()])
        .assert()
        .success();

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "store",
            "blob",
            "get",
            &url,
            &handle,
            output_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    let out = std::fs::read(&output_path).unwrap();
    assert_eq!(contents, &out[..]);
}

#[test]
fn store_blob_inspect_outputs_metadata() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("inspect.bin");
    let contents = b"remote";
    std::fs::write(&file_path, contents).unwrap();

    let url = format!("file://{}", dir.path().display());

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\\n$");

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "put", &url, file_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match(&pattern).unwrap());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "blob", "inspect", &url, &handle])
        .assert()
        .success()
        .stdout(predicate::str::contains("Length:"));
}

#[test]
fn store_pin_list_outputs_id() {
    let dir = tempdir().unwrap();
    let pin_id = [1u8; 16];
    let pin_hex = hex::encode(pin_id);
    let pins_dir = dir.path().join("pins");
    std::fs::create_dir_all(&pins_dir).unwrap();
    std::fs::write(pins_dir.join(&pin_hex), b"pin").unwrap();

    let url = format!("file://{}", dir.path().display());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["store", "pin", "list", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains(pin_hex.to_ascii_uppercase()));
}
