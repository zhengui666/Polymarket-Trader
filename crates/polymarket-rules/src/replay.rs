use polymarket_core::{ReplayReport, ReplayRequest};

pub fn build_replay_report(
    request: &ReplayRequest,
    processed_events: usize,
    graph_versions: Vec<String>,
) -> ReplayReport {
    ReplayReport {
        run_id: uuid::Uuid::new_v4(),
        requested_at: chrono::Utc::now(),
        completed_at: chrono::Utc::now(),
        event_limit: request.limit,
        processed_events,
        graph_versions,
    }
}
