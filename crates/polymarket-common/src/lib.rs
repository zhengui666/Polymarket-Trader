use tokio::signal;

pub fn init_tracing(filter: &str) {
    init_tracing_with_sampling(filter, 1.0);
}

pub fn init_tracing_with_sampling(filter: &str, sample_ratio: f64) {
    let _sample_ratio = sample_ratio.clamp(0.0, 1.0);
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .compact()
        .with_ansi(true)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .try_init();
}

pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let ctrl_c = signal::ctrl_c();
        let terminate = async {
            if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
                sigterm.recv().await;
            }
        };
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}
