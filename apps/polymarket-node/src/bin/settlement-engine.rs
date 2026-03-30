fn main() -> anyhow::Result<()> {
    polymarket_services::run_service(polymarket_core::ServiceKind::SettlementEngine)
}
