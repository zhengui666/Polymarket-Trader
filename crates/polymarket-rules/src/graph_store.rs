use polymarket_core::{
    ConstraintGraphSnapshot, EventFamilySnapshot, MarketCanonical, ReplayReport,
};

#[derive(Debug, Clone, Default)]
pub struct RulesStoreBundle {
    pub market: Option<MarketCanonical>,
    pub family: Option<EventFamilySnapshot>,
    pub graph: Option<ConstraintGraphSnapshot>,
    pub replay: Option<ReplayReport>,
}
