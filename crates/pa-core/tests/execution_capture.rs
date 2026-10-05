//! Live regression: the queued execute path must capture both protocol directions.
#![cfg(unix)]

use pa_core::kernel::{
    manager::KernelStartOptions,
    shared::{ExecuteOptions, ExecuteStatus},
    KernelManagerOptions, KernelShutdownOptions, ReplKernelManager,
};
use pa_types::diagnostics::{ExecutionTrace, TracePoint};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

#[test]
#[ignore = "requires PA_CORE_KERNEL_PYTHON with the real Prime Agent runtime installed"]
fn captures_queued_code_and_its_actual_kernel_output() {
    if std::env::var_os("PA_EXECUTION_CAPTURE_TEST_CHILD").is_none() {
        let directory = tempfile::tempdir().unwrap();
        pa_core::platform::perms::restrict_dir(directory.path()).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "captures_queued_code_and_its_actual_kernel_output",
                "--ignored",
                "--nocapture",
            ])
            .env("PA_EXECUTION_CAPTURE_TEST_CHILD", "1")
            .env("PRIME_AGENT_DEBUG_TRACE_DIR", directory.path())
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    // The recorder's process-wide opt-in is initialized in a separate process,
    // never by mutating the test runner's environment after threads start.
    let python = PathBuf::from(
        std::env::var_os("PA_CORE_KERNEL_PYTHON").expect("set the live kernel interpreter"),
    );
    let directory = PathBuf::from(std::env::var_os("PRIME_AGENT_DEBUG_TRACE_DIR").unwrap());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let manager = ReplKernelManager::new(KernelManagerOptions {
                python: Some(python),
                ..KernelManagerOptions::default()
            });
            manager.start(KernelStartOptions::default()).await.unwrap();
            let result = manager
                .execute("print(21 * 2)", ExecuteOptions::default())
                .await
                .unwrap();
            assert_eq!(
                (result.status, result.stdout),
                (ExecuteStatus::Ok, "42\n".into())
            );
            manager
                .shutdown(KernelShutdownOptions::default())
                .await
                .unwrap();
        });
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let mut records = Vec::new();
        for entry in std::fs::read_dir(&directory).unwrap() {
            let bytes = std::fs::read(entry.unwrap().path()).unwrap();
            for line in bytes.split_inclusive(|byte| *byte == b'\n') {
                if line.last() == Some(&b'\n') {
                    records.push(serde_json::from_slice::<ExecutionTrace>(line).unwrap());
                }
            }
        }
        if let Some(sent) = records.iter().find(|record| {
            record.point == TracePoint::KernelSend && record.payload["code"] == "print(21 * 2)"
        }) {
            let received: String = records
                .iter()
                .filter(|record| {
                    record.point == TracePoint::KernelReceive
                        && record.payload["id"] == sent.payload["id"]
                        && record.payload["event"] == "stdout"
                })
                .filter_map(|record| record.payload["text"].as_str())
                .collect();
            if received == "42\n" {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "missing queued execute or correlated stdout trace"
        );
        std::thread::yield_now();
    }
}
