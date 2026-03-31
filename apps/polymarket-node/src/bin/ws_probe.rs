use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() -> Result<()> {
    let response =
        reqwest::get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100")
            .await?
            .error_for_status()?;
    let payload = response.json::<Value>().await?;
    let items = payload.as_array().cloned().unwrap_or_default();
    let mut assets = Vec::new();
    for item in items {
        let raw = item
            .get("clobTokenIds")
            .or_else(|| item.get("tokenIds"))
            .or_else(|| item.get("token_ids"));
        match raw {
            Some(Value::Array(values)) => {
                assets.extend(
                    values
                        .iter()
                        .filter_map(|value| value.as_str().map(str::to_owned)),
                );
            }
            Some(Value::String(value)) => {
                if let Ok(parsed) = serde_json::from_str::<Vec<String>>(value) {
                    assets.extend(parsed);
                }
            }
            _ => {}
        }
    }
    assets.truncate(100);
    let (mut stream, _) =
        connect_async("wss://ws-subscriptions-clob.polymarket.com/ws/market").await?;
    for chunk in assets.chunks(50) {
        stream
            .send(Message::Text(
                json!({
                    "assets_ids": chunk,
                    "type": "market",
                    "custom_feature_enabled": true,
                })
                .to_string()
                .into(),
            ))
            .await?;
    }
    println!("probe subscribed assets={}", assets.len());
    for index in 0..3 {
        match tokio::time::timeout(std::time::Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(message))) => match message {
                Message::Text(text) => {
                    println!("message[{index}] text {}", text);
                }
                Message::Ping(payload) => {
                    println!(
                        "message[{index}] ping {}",
                        String::from_utf8_lossy(&payload)
                    );
                }
                Message::Pong(payload) => {
                    println!(
                        "message[{index}] pong {}",
                        String::from_utf8_lossy(&payload)
                    );
                }
                other => {
                    println!("message[{index}] other {:?}", other);
                }
            },
            Ok(Some(Err(error))) => {
                println!("probe error {error}");
                break;
            }
            Ok(None) => {
                println!("probe stream closed");
                break;
            }
            Err(_) => {
                println!("probe timeout");
                break;
            }
        }
    }
    Ok(())
}
