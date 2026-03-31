use anyhow::Result;
use polymarket_config::NodeConfig;
use polymarket_core::{AccountDomain, ConstraintGraphSnapshot};
use polymarket_storage::Store;
use rusqlite::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    let selected_domain = std::env::var("POLYMARKET_NODE_DOMAIN")
        .ok()
        .unwrap_or_else(|| "SIM".to_owned())
        .parse::<AccountDomain>()?;
    let config = NodeConfig::from_env(selected_domain)?;
    let db_path = &config.selected_domain_config.database_path;
    let store = Store::new(
        db_path.clone(),
        config.selected_domain,
        config.selected_domain_config.namespace(),
        config.selected_domain_config.audit_prefix.clone(),
    );
    let connection = Connection::open(db_path)?;
    let mut statement = connection.prepare(
        r#"
        SELECT payload_json
        FROM state_snapshots
        WHERE aggregate_type = 'rules_constraint_graph'
        ORDER BY created_at ASC
        "#,
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut restored = 0usize;
    for row in rows {
        let payload_json = row?;
        let snapshot: ConstraintGraphSnapshot = serde_json::from_str(&payload_json)?;
        store.upsert_constraint_graph_snapshot(snapshot)?;
        restored += 1;
    }
    println!("restored_constraint_graphs={restored}");
    Ok(())
}
