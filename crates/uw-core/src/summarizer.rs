use async_trait::async_trait;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummarizeTemplate {
    pub preserve: String,
    pub discard: String,
}

impl Default for SummarizeTemplate {
    fn default() -> Self {
        Self {
            preserve: "the current goal, the step in progress and its exact next action, decisions already made and why, and every approach already tried and rejected".into(),
            discard: "raw file contents already read, superseded plans, and tool output that has already been acted on".into(),
        }
    }
}

impl SummarizeTemplate {
    pub fn prompt(&self, transcript: &str) -> String {
        format!(
            "Summarize this coding-agent session for a lean continuation.\n\nPreserve: {}.\nDiscard: {}.\n\nTranscript:\n{}",
            self.preserve, self.discard, transcript
        )
    }
}

#[derive(Debug, Error)]
pub enum SummarizerError {
    #[error("summarizer transport failed: {0}")]
    Transport(String),
    #[error("summarizer returned HTTP status {0}")]
    Http(u16),
    #[error("malformed summarizer response: {0}")]
    Malformed(String),
}

#[async_trait]
pub trait Summarizer: Send + Sync {
    async fn summarize(
        &self,
        transcript: &str,
        template: &SummarizeTemplate,
    ) -> Result<String, SummarizerError>;
}
