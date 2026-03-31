use std::collections::BTreeMap;

use anyhow::{Context, Result};
use polymarket_config::TelegramConfig;
use polymarket_core::{now, AccountDomain};
use polymarket_services::{NotificationEvent, Notifier, RiskAlertNotification, TelegramNotifier};

#[tokio::main]
async fn main() -> Result<()> {
    let vars = std::env::vars().collect::<BTreeMap<_, _>>();
    let telegram = TelegramConfig::from_map(&vars).context("failed to load telegram config")?;
    let notifier = TelegramNotifier::from_config(&telegram)?;
    let event = NotificationEvent::RiskAlert(RiskAlertNotification {
        domain: AccountDomain::Sim,
        code: "MANUAL_TRIGGER".to_owned(),
        severity: "INFO".to_owned(),
        scope: "system".to_owned(),
        message: "manual in-app Telegram notification trigger".to_owned(),
        current_value: Some("sim-runtime".to_owned()),
        threshold: Some("telegram-enabled".to_owned()),
        system_action: "send_test_notification".to_owned(),
        occurred_at: now(),
        dedupe_key: format!("manual-trigger-{}", now().timestamp_millis()),
    });

    println!(
        "trigger_notification starting domain={} kind={:?}",
        event.domain(),
        event.kind()
    );
    notifier.send(event).await?;
    println!("trigger_notification delivered");
    Ok(())
}
