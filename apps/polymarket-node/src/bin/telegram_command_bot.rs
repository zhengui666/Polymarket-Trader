use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use polymarket_config::NodeConfig;
use polymarket_core::AccountDomain;
use reqwest::Client;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use tokio::time::sleep;

const TELEGRAM_API_BASE: &str = "https://api.telegram.org";

#[tokio::main]
async fn main() -> Result<()> {
    let selected_domain = std::env::var("POLYMARKET_NODE_DOMAIN")
        .ok()
        .as_deref()
        .map(AccountDomain::from_str)
        .transpose()?
        .unwrap_or(AccountDomain::Sim);
    let config = NodeConfig::from_env(selected_domain)?;
    let telegram = config.telegram.clone();
    let bot_token = telegram
        .bot_token
        .resolve_string()?
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("telegram bot token is empty"))?;
    let allowed_chat_id = telegram
        .chat_id
        .resolve_string()?
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("telegram chat id is empty"))?;
    let db_path = config.selected_domain_config.database_path.clone();
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to build telegram command bot client")?;

    println!(
        "telegram_command_bot started domain={} db_path={}",
        selected_domain,
        db_path.display()
    );

    let mut offset: i64 = bootstrap_offset(&client, &bot_token).await?;
    loop {
        match poll_once(
            &client,
            &bot_token,
            &allowed_chat_id,
            selected_domain,
            db_path.as_path(),
            &mut offset,
        )
        .await
        {
            Ok(()) => {}
            Err(error) => eprintln!("telegram_command_bot poll error: {error:#}"),
        }
        sleep(Duration::from_secs(2)).await;
    }
}

async fn bootstrap_offset(client: &Client, bot_token: &str) -> Result<i64> {
    let updates = fetch_updates(client, bot_token, None).await?;
    Ok(updates
        .iter()
        .map(|update| update.update_id + 1)
        .max()
        .unwrap_or(0))
}

async fn poll_once(
    client: &Client,
    bot_token: &str,
    allowed_chat_id: &str,
    domain: AccountDomain,
    db_path: &std::path::Path,
    offset: &mut i64,
) -> Result<()> {
    let updates = fetch_updates(client, bot_token, Some(*offset)).await?;
    for update in updates {
        *offset = (*offset).max(update.update_id + 1);
        let Some(message) = update.message else {
            continue;
        };
        if message.chat.id.to_string() != allowed_chat_id {
            continue;
        }
        let text = message.text.unwrap_or_default();
        if text.trim().eq_ignore_ascii_case("/position") {
            let body = render_position_summary(domain, db_path)?;
            send_message(client, bot_token, allowed_chat_id, &body).await?;
            println!("telegram_command_bot handled /position for chat_id={allowed_chat_id}");
        } else if let Some(research_ref) = parse_research_command(&text) {
            let body = render_research_summary(domain, db_path, research_ref)?;
            send_message(client, bot_token, allowed_chat_id, &body).await?;
            println!(
                "telegram_command_bot handled /research for chat_id={allowed_chat_id} ref={research_ref}"
            );
        }
    }
    Ok(())
}

async fn fetch_updates(
    client: &Client,
    bot_token: &str,
    offset: Option<i64>,
) -> Result<Vec<TelegramUpdate>> {
    let mut request = client.get(format!("{TELEGRAM_API_BASE}/bot{bot_token}/getUpdates"));
    if let Some(offset) = offset {
        request = request.query(&[("offset", offset), ("timeout", 25_i64)]);
    } else {
        request = request.query(&[("timeout", 1_i64)]);
    }
    let response = request
        .send()
        .await
        .context("failed to fetch telegram updates")?
        .error_for_status()
        .context("telegram getUpdates returned error status")?;
    let payload: TelegramResponse<Vec<TelegramUpdate>> = response
        .json()
        .await
        .context("failed to decode telegram updates payload")?;
    Ok(payload.result)
}

async fn send_message(client: &Client, bot_token: &str, chat_id: &str, text: &str) -> Result<()> {
    client
        .post(format!("{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"))
        .json(&serde_json::json!({
            "chat_id": chat_id,
            "text": text,
        }))
        .send()
        .await
        .context("failed to send telegram command response")?
        .error_for_status()
        .context("telegram sendMessage returned error status")?;
    Ok(())
}

fn parse_research_command(text: &str) -> Option<&str> {
    text.trim()
        .strip_prefix("/research")
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn render_position_summary(domain: AccountDomain, db_path: &std::path::Path) -> Result<String> {
    let connection = Connection::open(db_path)
        .with_context(|| format!("failed to open sqlite db at {}", db_path.display()))?;
    if domain == AccountDomain::Sim {
        return render_sim_position_summary(&connection, domain);
    }
    let payload_json: String = connection
        .query_row(
            "SELECT payload_json
             FROM state_snapshots
             WHERE domain = ?1
               AND aggregate_type = 'portfolio_snapshot'
               AND aggregate_id = 'current'
             ORDER BY created_at DESC
             LIMIT 1",
            params![domain.as_str()],
            |row| row.get(0),
        )
        .context("failed to load latest portfolio_snapshot")?;
    let payload: Value =
        serde_json::from_str(&payload_json).context("failed to parse portfolio snapshot json")?;

    let cash_available = payload
        .get("cash_available")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let nav = payload
        .get("nav")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let reserved_cash = payload
        .get("reserved_cash")
        .and_then(Value::as_f64)
        .unwrap_or_default();
    let positions = payload
        .get("positions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut lines = vec![
        format!("[{}] 当前仓位", domain.as_str()),
        format!("NAV: {:.4}", nav),
        format!("Cash Available: {:.4}", cash_available),
        format!("Reserved Cash: {:.4}", reserved_cash),
        format!("Positions: {}", positions.len()),
    ];

    if positions.is_empty() {
        lines.push("当前无持仓".to_owned());
        return Ok(lines.join("\n"));
    }

    for (index, position) in positions.iter().take(10).enumerate() {
        let market_id = position
            .get("market_id")
            .and_then(Value::as_str)
            .unwrap_or("-");
        let token_id = position
            .get("token_id")
            .and_then(Value::as_str)
            .unwrap_or("-");
        let quantity = position
            .get("quantity")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let average_price = position
            .get("average_price")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let mark_price = position
            .get("mark_price")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        let unrealized_pnl = position
            .get("unrealized_pnl")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        lines.push(format!(
            "{}. qty={:.4} avg={:.4} mark={:.4} upnl={:.4}",
            index + 1,
            quantity,
            average_price,
            mark_price,
            unrealized_pnl
        ));
        lines.push(format!("market={}", shorten(market_id)));
        lines.push(format!("token={}", shorten(token_id)));
    }

    Ok(lines.join("\n"))
}

fn render_research_summary(
    domain: AccountDomain,
    db_path: &std::path::Path,
    research_ref: &str,
) -> Result<String> {
    let connection = Connection::open(db_path)
        .with_context(|| format!("failed to open sqlite db at {}", db_path.display()))?;
    let row: Option<(String, String)> = connection
        .query_row(
            "SELECT payload_json, created_at
             FROM state_snapshots
             WHERE domain = ?1 AND aggregate_type = 'research_asset' AND aggregate_id = ?2",
            params![domain.as_str(), research_ref],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .context("failed to query research asset snapshot")?;
    let Some((payload_json, created_at)) = row else {
        return Ok(format!("未找到 research asset: {research_ref}"));
    };
    let payload: Value =
        serde_json::from_str(&payload_json).context("failed to decode research asset payload")?;
    let summary = payload
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("无摘要");
    let contradictions = join_json_string_array(payload.get("contradictions"));
    let risks = join_json_string_array(payload.get("risks"));
    let evidence_gaps = join_json_string_array(payload.get("evidence_gaps"));
    let mut lines = vec![
        format!("research_ref: {research_ref}"),
        format!("generated_at: {created_at}"),
        format!("summary: {summary}"),
    ];
    if let Some(value) = contradictions {
        lines.push(format!("contradictions: {value}"));
    }
    if let Some(value) = risks {
        lines.push(format!("risks: {value}"));
    }
    if let Some(value) = evidence_gaps {
        lines.push(format!("evidence_gaps: {value}"));
    }
    if let Some(links) = payload.get("source_links").and_then(Value::as_array) {
        let compact = links
            .iter()
            .filter_map(Value::as_str)
            .take(3)
            .collect::<Vec<_>>()
            .join(" | ");
        if !compact.is_empty() {
            lines.push(format!("links: {compact}"));
        }
    }
    Ok(lines.join("\n"))
}

fn join_json_string_array(value: Option<&Value>) -> Option<String> {
    let joined = value?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join(" | ");
    if joined.is_empty() {
        None
    } else {
        Some(joined)
    }
}

fn render_sim_position_summary(connection: &Connection, domain: AccountDomain) -> Result<String> {
    let mut statement = connection.prepare(
        "SELECT market_id,
                json_extract(detail_json, '$.side') AS side,
                json_extract(detail_json, '$.token_id') AS token_id,
                json_extract(detail_json, '$.limit_price') AS limit_price,
                filled_quantity
         FROM sim_orders
         WHERE json_extract(detail_json, '$.side') IS NOT NULL
         ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<f64>>(3)?,
            row.get::<_, f64>(4)?,
        ))
    })?;

    let mut positions: std::collections::BTreeMap<(String, String), (f64, f64)> =
        std::collections::BTreeMap::new();
    for row in rows {
        let (market_id, side, token_id, limit_price, filled_quantity) = row?;
        let token_id = token_id.unwrap_or_else(|| market_id.clone());
        let signed_quantity = if side.eq_ignore_ascii_case("SELL") {
            -filled_quantity
        } else {
            filled_quantity
        };
        let entry = positions
            .entry((market_id, token_id))
            .or_insert((0.0_f64, 0.0_f64));
        entry.0 += signed_quantity;
        entry.1 += limit_price.unwrap_or_default() * filled_quantity;
    }

    let mut lines = vec![format!("[{}] 当前仓位", domain.as_str())];
    let non_zero = positions
        .into_iter()
        .filter(|(_, (quantity, _))| quantity.abs() > f64::EPSILON)
        .collect::<Vec<_>>();
    lines.push(format!("Positions: {}", non_zero.len()));

    if non_zero.is_empty() {
        lines.push("当前无持仓".to_owned());
        return Ok(lines.join("\n"));
    }

    for (index, ((market_id, token_id), (quantity, notional))) in
        non_zero.iter().take(10).enumerate()
    {
        let average_price = if quantity.abs() > f64::EPSILON {
            notional / quantity.abs()
        } else {
            0.0
        };
        lines.push(format!(
            "{}. qty={:.4} avg={:.4}",
            index + 1,
            quantity,
            average_price
        ));
        lines.push(format!("market={}", shorten(market_id)));
        lines.push(format!("token={}", shorten(token_id)));
    }

    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::{join_json_string_array, parse_research_command};
    use serde_json::json;

    #[test]
    fn parse_research_command_extracts_ref() {
        assert_eq!(
            parse_research_command("/research research:abc-123"),
            Some("research:abc-123")
        );
        assert_eq!(parse_research_command("/research"), None);
        assert_eq!(parse_research_command("/position"), None);
    }

    #[test]
    fn join_json_string_array_skips_empty_values() {
        assert_eq!(
            join_json_string_array(Some(&json!(["a", "b"]))),
            Some("a | b".to_string())
        );
        assert_eq!(join_json_string_array(Some(&json!([]))), None);
        assert_eq!(join_json_string_array(Some(&json!("oops"))), None);
    }
}

fn shorten(value: &str) -> String {
    if value.len() <= 18 {
        return value.to_owned();
    }
    format!("{}...{}", &value[..8], &value[value.len() - 6..])
}

#[derive(Debug, serde::Deserialize)]
struct TelegramResponse<T> {
    result: T,
}

#[derive(Debug, serde::Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    message: Option<TelegramMessage>,
}

#[derive(Debug, serde::Deserialize)]
struct TelegramMessage {
    text: Option<String>,
    chat: TelegramChat,
}

#[derive(Debug, serde::Deserialize)]
struct TelegramChat {
    id: i64,
}
