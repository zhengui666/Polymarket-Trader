use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use polymarket_config::TelegramConfig;
use polymarket_core::{AccountDomain, Timestamp};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NotificationKind {
    ClosePnl,
    DailyReport,
    RiskAlert,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClosePnlNotification {
    pub domain: AccountDomain,
    pub market_id: String,
    pub token_id: String,
    pub side: String,
    pub closed_quantity: f64,
    pub entry_average_price: f64,
    pub exit_average_price: f64,
    pub gross_realized_pnl: f64,
    pub entry_fees_allocated: f64,
    pub exit_fees: f64,
    pub total_fees: f64,
    pub net_realized_pnl: f64,
    pub occurred_at: Timestamp,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DailyReportNotification {
    pub domain: AccountDomain,
    pub report_date: String,
    pub timezone: String,
    pub close_count: usize,
    pub win_count: usize,
    pub gross_realized_pnl: f64,
    pub total_fees: f64,
    pub net_realized_pnl: f64,
    pub best_trade_pnl: f64,
    pub worst_trade_pnl: f64,
    pub severe_risk_event_count: usize,
    pub generated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskAlertNotification {
    pub domain: AccountDomain,
    pub code: String,
    pub severity: String,
    pub scope: String,
    pub message: String,
    pub current_value: Option<String>,
    pub threshold: Option<String>,
    pub system_action: String,
    pub occurred_at: Timestamp,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NotificationEvent {
    ClosePnl(ClosePnlNotification),
    DailyReport(DailyReportNotification),
    RiskAlert(RiskAlertNotification),
}

impl NotificationEvent {
    pub fn kind(&self) -> NotificationKind {
        match self {
            Self::ClosePnl(_) => NotificationKind::ClosePnl,
            Self::DailyReport(_) => NotificationKind::DailyReport,
            Self::RiskAlert(_) => NotificationKind::RiskAlert,
        }
    }

    fn render(&self) -> String {
        match self {
            Self::ClosePnl(item) => format!(
                "[{}] 平仓收益通知\n市场: {}\nToken: {}\n方向: {}\n本次平仓数量: {:.4}\n开仓均价: {:.4}\n平仓均价: {:.4}\n毛收益: {:.4}\n手续费: {:.4} (开仓分摊 {:.4} + 平仓 {:.4})\n净收益: {:.4}\n时间: {}",
                item.domain,
                item.market_id,
                item.token_id,
                item.side,
                item.closed_quantity,
                item.entry_average_price,
                item.exit_average_price,
                item.gross_realized_pnl,
                item.total_fees,
                item.entry_fees_allocated,
                item.exit_fees,
                item.net_realized_pnl,
                item.occurred_at
            ),
            Self::DailyReport(item) => {
                let win_rate = if item.close_count == 0 {
                    0.0
                } else {
                    item.win_count as f64 / item.close_count as f64 * 100.0
                };
                format!(
                    "[{}] 每日交易日报\n日期: {} ({})\n平仓笔数: {}\n胜率: {:.2}%\n已实现毛盈亏: {:.4}\n手续费总额: {:.4}\n净收益: {:.4}\n最大单笔盈利: {:.4}\n最大单笔亏损: {:.4}\n严重风险事件: {}\n生成时间: {}",
                    item.domain,
                    item.report_date,
                    item.timezone,
                    item.close_count,
                    win_rate,
                    item.gross_realized_pnl,
                    item.total_fees,
                    item.net_realized_pnl,
                    item.best_trade_pnl,
                    item.worst_trade_pnl,
                    item.severe_risk_event_count,
                    item.generated_at
                )
            }
            Self::RiskAlert(item) => format!(
                "[{}] 系统风险告警\n风险代码: {}\n严重级别: {}\n影响范围: {}\n说明: {}\n当前值: {}\n阈值: {}\n系统动作: {}\n时间: {}",
                item.domain,
                item.code,
                item.severity,
                item.scope,
                item.message,
                item.current_value.as_deref().unwrap_or("N/A"),
                item.threshold.as_deref().unwrap_or("N/A"),
                item.system_action,
                item.occurred_at
            ),
        }
    }
}

#[async_trait]
pub trait Notifier: Send + Sync {
    async fn send(&self, event: NotificationEvent) -> Result<()>;
}

pub struct TelegramNotifier {
    enabled: bool,
    client: Client,
    bot_token: Option<String>,
    chat_id: Option<String>,
}

impl TelegramNotifier {
    pub fn from_config(config: &TelegramConfig) -> Result<Self> {
        let client = Client::builder()
            .build()
            .context("failed to build telegram notifier client")?;
        if !config.enabled {
            return Ok(Self {
                enabled: false,
                client,
                bot_token: None,
                chat_id: None,
            });
        }

        let bot_token = config
            .bot_token
            .resolve_string()?
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("telegram bot token is empty"))?;
        let chat_id = config
            .chat_id
            .resolve_string()?
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("telegram chat id is empty"))?;

        Ok(Self {
            enabled: true,
            client,
            bot_token: Some(bot_token),
            chat_id: Some(chat_id),
        })
    }
}

#[async_trait]
impl Notifier for TelegramNotifier {
    async fn send(&self, event: NotificationEvent) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token.as_deref().unwrap_or_default()
        );
        let response = self
            .client
            .post(url)
            .json(&json!({
                "chat_id": self.chat_id.as_deref().unwrap_or_default(),
                "text": event.render(),
            }))
            .send()
            .await
            .context("failed to send telegram message")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read telegram response body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "telegram sendMessage failed: status={} body={}",
                status,
                body
            ));
        }
        Ok(())
    }
}
