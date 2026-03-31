use anyhow::Result;
use polymarket_common::shutdown_signal;
use polymarket_config::NodeConfig;
use polymarket_core::AccountDomain;
use polymarket_services::NodeRuntime;
use rustls::crypto::aws_lc_rs;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = aws_lc_rs::default_provider().install_default();
    let domain = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("POLYMARKET_NODE_DOMAIN").ok())
        .unwrap_or_else(|| "SIM".to_owned())
        .parse::<AccountDomain>()?;
    let config = NodeConfig::from_env(domain)?;
    let runtime = NodeRuntime::new(config);
    let cancellation = CancellationToken::new();
    let shutdown = cancellation.clone();

    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown.cancel();
    });

    runtime.run_until_cancelled(cancellation).await
}
