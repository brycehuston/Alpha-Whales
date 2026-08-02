use thiserror::Error;

#[derive(Error, Debug)]
pub enum BotError {
    #[error("Configuration Error: {0}")]
    ConfigError(String),

    #[error("WebSocket Error: {0}")]
    WebSocketError(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Execution HTTP Error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Deserialization Error: {0}")]
    #[allow(dead_code)] // Reserved error variant for future JSON parsing error propagation
    ParseError(String),
}
