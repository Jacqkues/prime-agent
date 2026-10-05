use super::*;

#[cfg(unix)]
#[test]
fn captures_real_payloads_in_private_files_and_redacts_credentials() {
    let dir = tempfile::tempdir().unwrap();
    crate::platform::perms::restrict_dir(dir.path()).unwrap();
    let recorder = Recorder::start(dir.path().to_path_buf()).unwrap();
    recorder.record(
        TracePoint::KernelSend,
        &json!({"id":"exec-1","code":"print(21 * 2)","workerToken":"secret"}),
    );
    let path = dir
        .path()
        .join(format!("trace-{}-0.jsonl", recorder.instance));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let record = loop {
        let bytes = std::fs::read(&path).unwrap();
        if bytes.last() == Some(&b'\n') {
            break serde_json::from_slice::<ExecutionTrace>(&bytes).unwrap();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "writer did not publish a complete record"
        );
        std::thread::yield_now();
    };
    assert_eq!(
        record.payload,
        json!({"id":"exec-1","code":"print(21 * 2)","workerToken":"[redacted]"})
    );
    assert_eq!(
        (
            record.point,
            record.correlation_id.as_deref(),
            record.truncated,
            record.dropped_total
        ),
        (TracePoint::KernelSend, Some("exec-1"), false, 0)
    );
    assert_eq!(crate::platform::perms::file_mode(&path), Some(0o600));
}

#[cfg(unix)]
#[test]
fn capture_refuses_a_public_or_symlinked_destination() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Recorder::start(dir.path().to_path_buf()).is_err());
    crate::platform::perms::restrict_dir(dir.path()).unwrap();
    let link = dir.path().join("link");
    symlink(dir.path(), &link).unwrap();
    assert!(Recorder::start(link).is_err());
}
