use thiserror::Error;

#[derive(Debug, Error)]
pub enum OpportunityError {
    #[error("missing order book for market `{0}`")]
    MissingBook(String),
    #[error("invalid scan input: {0}")]
    InvalidInput(String),
}
