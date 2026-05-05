//! Error types for the RTS pipeline.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RtsError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid event: {0}")]
    InvalidEvent(String),
}
