use thiserror::Error;

#[derive(Debug, Error)]
pub enum RulesError {
    #[error("invalid market document: {0}")]
    InvalidDocument(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
