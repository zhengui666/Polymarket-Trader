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
    pub fn domain(&self) -> AccountDomain {
        match self {
            Self::ClosePnl(notification) => notification.domain,
            Self::DailyReport(notification) => notification.domain,
            Self::RiskAlert(notification) => notification.domain,
        }
    }

    pub fn kind(&self) -> NotificationKind {
        match self {
            Self::ClosePnl(_) => NotificationKind::ClosePnl,
            Self::DailyReport(_) => NotificationKind::DailyReport,
            Self::RiskAlert(_) => NotificationKind::RiskAlert,
        }
    }

    #[allow(dead_code)]
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

pub struct NotificationRenderer;

impl NotificationRenderer {
    pub fn render_telegram(event: &NotificationEvent) -> String {
        match event {
            NotificationEvent::ClosePnl(item) => format!(
                "<b>[{}] 平仓收益</b>\n<pre>市场      {}</pre>\n<pre>Token     {}</pre>\n<pre>方向      {}</pre>\n<pre>数量      {:.4}</pre>\n<pre>开仓均价  {:.4}</pre>\n<pre>平仓均价  {:.4}</pre>\n<pre>毛收益    {:.4}</pre>\n<pre>手续费    {:.4}</pre>\n<pre>净收益    {:.4}</pre>\n\n<b>费用拆分</b>\n开仓分摊 {:.4} / 平仓 {:.4}\n\n<b>时间</b>\n{}",
                item.domain,
                escape_html(&item.market_id),
                escape_html(&item.token_id),
                escape_html(&item.side),
                item.closed_quantity,
                item.entry_average_price,
                item.exit_average_price,
                item.gross_realized_pnl,
                item.total_fees,
                item.net_realized_pnl,
                item.entry_fees_allocated,
                item.exit_fees,
                item.occurred_at
            ),
            NotificationEvent::DailyReport(item) => {
                let win_rate = if item.close_count == 0 {
                    0.0
                } else {
                    item.win_count as f64 / item.close_count as f64 * 100.0
                };
                format!(
                    "<b>[{}] 每日交易日报</b>\n<b>日期</b> {} ({})\n\n<pre>平仓笔数  {}</pre>\n<pre>胜率      {:.2}%</pre>\n<pre>毛盈亏    {:.4}</pre>\n<pre>手续费    {:.4}</pre>\n<pre>净盈亏    {:.4}</pre>\n<pre>最佳交易  {:.4}</pre>\n<pre>最差交易  {:.4}</pre>\n<pre>严重风险  {}</pre>\n\n<b>生成时间</b>\n{}",
                    item.domain,
                    escape_html(&item.report_date),
                    escape_html(&item.timezone),
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
            NotificationEvent::RiskAlert(item) => format!(
                "<b>[{}] 风险告警</b>\n<b>等级</b> {}\n<b>代码</b> <code>{}</code>\n<b>范围</b> {}\n\n<b>消息</b>\n{}\n\n<pre>当前值    {}</pre>\n<pre>阈值      {}</pre>\n<pre>系统动作  {}</pre>\n<pre>去重键    {}</pre>\n\n<b>时间</b>\n{}",
                item.domain,
                escape_html(&item.severity),
                escape_html(&item.code),
                escape_html(&item.scope),
                escape_html(&item.message),
                escape_html(item.current_value.as_deref().unwrap_or("-")),
                escape_html(item.threshold.as_deref().unwrap_or("-")),
                escape_html(&item.system_action),
                escape_html(&item.dedupe_key),
                item.occurred_at
            ),
        }
    }

    pub fn telegram_chunks(event: &NotificationEvent) -> Vec<String> {
        chunk_message(&Self::render_telegram(event), 3_800)
    }
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn chunk_message(message: &str, limit: usize) -> Vec<String> {
    if message.chars().count() <= limit {
        return vec![message.to_owned()];
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    for line in message.lines() {
        if line.chars().count() > limit {
            if !current.is_empty() {
                chunks.push(current);
                current = String::new();
            }
            let mut partial = String::new();
            for ch in line.chars() {
                if partial.chars().count() >= limit {
                    chunks.push(partial);
                    partial = String::new();
                }
                partial.push(ch);
            }
            if !partial.is_empty() {
                chunks.push(partial);
            }
            continue;
        }
        let next_len = current.chars().count() + line.chars().count() + 1;
        if !current.is_empty() && next_len > limit {
            chunks.push(current);
            current = String::new();
        }
        if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(line);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    if chunks.is_empty() {
        vec![message.chars().take(limit).collect()]
    } else {
        chunks
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
    allowed_domains: Vec<AccountDomain>,
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
                allowed_domains: Vec::new(),
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
            allowed_domains: config.allowed_domains.iter().copied().collect(),
        })
    }
}

#[async_trait]
impl Notifier for TelegramNotifier {
    async fn send(&self, event: NotificationEvent) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if !self.allowed_domains.contains(&event.domain()) {
            return Ok(());
        }

        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token.as_deref().unwrap_or_default()
        );
        for chunk in NotificationRenderer::telegram_chunks(&event) {
            let response = self
                .client
                .post(&url)
                .json(&json!({
                    "chat_id": self.chat_id.as_deref().unwrap_or_default(),
                    "text": chunk,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": true,
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
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use polymarket_core::AccountDomain;

    use super::*;

    #[test]
    fn renderer_escapes_html() {
        let event = NotificationEvent::RiskAlert(RiskAlertNotification {
            domain: AccountDomain::Canary,
            code: "RISK<1>".to_owned(),
            severity: "HIGH".to_owned(),
            scope: "market&desk".to_owned(),
            message: "spread < limit & volatile".to_owned(),
            current_value: Some("1 < 2".to_owned()),
            threshold: Some("3 > 2".to_owned()),
            system_action: "observe".to_owned(),
            occurred_at: Utc::now(),
            dedupe_key: "dup&1".to_owned(),
        });

        let rendered = NotificationRenderer::render_telegram(&event);
        assert!(rendered.contains("RISK&lt;1&gt;"));
        assert!(rendered.contains("spread &lt; limit &amp; volatile"));
        assert!(rendered.contains("market&amp;desk"));
    }

    #[test]
    fn renderer_chunks_long_messages() {
        let event = NotificationEvent::RiskAlert(RiskAlertNotification {
            domain: AccountDomain::Canary,
            code: "RISK".to_owned(),
            severity: "HIGH".to_owned(),
            scope: "scope".to_owned(),
            message: "x".repeat(5000),
            current_value: None,
            threshold: None,
            system_action: "observe".to_owned(),
            occurred_at: Utc::now(),
            dedupe_key: "dup".to_owned(),
        });

        let chunks = NotificationRenderer::telegram_chunks(&event);
        assert!(chunks.len() >= 2);
        assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 3_800));
    }
}
