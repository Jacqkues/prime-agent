use super::*;

#[test]
fn retention_is_bounded_and_keeps_active_streams_before_completed_entries() {
    let inspector = Inspector::default();
    let mut active = inspector.begin("GET".into(), "/sessions/{id}/events".into(), None);
    active.responded(200, true);
    let id = active.trace.id.clone();
    for _ in 0..REQUEST_LIMIT + 9 {
        let mut entry = inspector.begin("GET".into(), "/sessions".into(), None);
        entry.responded(200, false);
    }
    let snapshot = inspector.snapshot();
    assert_eq!(
        (
            snapshot.requests.len(),
            snapshot.evicted_requests,
            snapshot.in_flight,
            snapshot.open_streams
        ),
        (REQUEST_LIMIT, 10, 1, 1)
    );
    assert!(snapshot.requests.iter().any(|entry| entry.id == id));
    drop(active);
    assert_eq!(
        (
            inspector.snapshot().in_flight,
            inspector.snapshot().open_streams
        ),
        (0, 0)
    );
}

#[test]
fn dropping_an_unanswered_request_records_interruption_without_a_false_http_status() {
    let inspector = Inspector::default();
    let request = inspector.begin("POST".into(), "/sessions".into(), None);
    drop(request);
    let snapshot = inspector.snapshot();
    assert_eq!(
        (
            snapshot.in_flight,
            snapshot.requests[0].status,
            snapshot.requests[0].response_ms
        ),
        (0, None, None)
    );
    assert!(snapshot.requests[0].ended_at_ms.is_some());
}
