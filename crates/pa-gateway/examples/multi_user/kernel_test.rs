use super::{catalog::Catalog, kernel::SharedKernel};
use pa_gateway::debug::Inspector;
use pa_types::gateway::Workspace;
use serde_json::{json, to_value};
use std::path::PathBuf;

#[tokio::test]
#[ignore = "requires PA_APP_KERNEL_PYTHON with prime-agent-runtime installed"]
async fn catalog_tracks_real_kernel_mutations_and_private_prompt_context() {
    let python =
        PathBuf::from(std::env::var_os("PA_APP_KERNEL_PYTHON").expect("set kernel interpreter"));
    let inspector = Inspector::default();
    let _capture = inspector.watch_traces(vec![]).unwrap();
    let kernel = SharedKernel::start(
        &python,
        inspector,
        Workspace {
            tenant_id: "test".into(),
            workspace_id: "catalog".into(),
        },
        &pa_telemetry::TelemetryClient::inert(),
    )
    .await
    .unwrap();
    let created = kernel.execute("def total(values):\n    \"\"\"Calculate the shared total.\"\"\"\n    return sum(values)\napp['secret_value'] = 'not-a-function'".into(), "Alice".into()).await.unwrap();
    let expected = json!({"entries":[{"name":"total","signature":"(values)","description":"Calculate the shared total.","kind":"function"}],"total":1,"truncated":false,"error":null});
    assert_eq!(
        to_value(&created.catalog.unwrap().catalog).unwrap(),
        expected
    );
    let snapshot = kernel.catalog().await;
    assert_eq!(to_value(&snapshot.catalog).unwrap(), expected);
    let context = snapshot.prompt_context();
    assert!(context.contains("Calculate the shared total."));
    assert!(!context.contains("not-a-function"));
    let called = kernel
        .execute("print(total([20, 22]))".into(), "Bob".into())
        .await
        .unwrap();
    assert_eq!(
        (called.status.as_str(), called.stdout.as_str()),
        ("ok", "42\n")
    );
    let changed = kernel.execute("async def total(value, factor=2):\n    \"\"\"New version.\"\"\"\n    return value * factor\nraise ValueError('partial cell')".into(), "Bob".into()).await.unwrap();
    assert_eq!(
        (
            changed.status.as_str(),
            to_value(changed.catalog.unwrap().catalog).unwrap()
        ),
        (
            "error",
            json!({"entries":[{"name":"total","signature":"(value, factor=Ellipsis)","description":"New version.","kind":"async_function"}],"total":1,"truncated":false,"error":null})
        )
    );
    let deleted = kernel
        .execute("del total".into(), "Alice".into())
        .await
        .unwrap();
    assert_eq!(
        to_value(deleted.catalog.unwrap().catalog).unwrap(),
        to_value(Catalog::default()).unwrap()
    );
    assert_eq!(kernel.catalog().await.kernel_id, snapshot.kernel_id);
}
