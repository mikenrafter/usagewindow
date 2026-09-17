use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use uw_core::summarizer::{SummarizeTemplate, Summarizer, SummarizerError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatResponse {
    pub status: u16,
    pub body: String,
}

#[async_trait]
pub trait ChatTransport: Send + Sync {
    async fn post(&self, request: ChatRequest) -> Result<ChatResponse, String>;
}

pub struct ReqwestChatTransport {
    client: reqwest::Client,
}
impl Default for ReqwestChatTransport {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}
#[async_trait]
impl ChatTransport for ReqwestChatTransport {
    async fn post(&self, request: ChatRequest) -> Result<ChatResponse, String> {
        let mut req = self.client.post(request.url).body(request.body);
        for (key, value) in request.headers {
            req = req.header(key, value);
        }
        let response = req.send().await.map_err(|e| e.to_string())?;
        let status = response.status().as_u16();
        Ok(ChatResponse {
            status,
            body: response.text().await.map_err(|e| e.to_string())?,
        })
    }
}

pub struct OpenAiCompatibleSummarizer {
    pub base_url: String,
    pub model: String,
    pub api_key: Option<String>,
    pub transport: Arc<dyn ChatTransport>,
}
impl OpenAiCompatibleSummarizer {
    pub fn new(
        base_url: impl Into<String>,
        model: impl Into<String>,
        api_key: Option<String>,
        transport: Arc<dyn ChatTransport>,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').into(),
            model: model.into(),
            api_key,
            transport,
        }
    }
}
#[async_trait]
impl Summarizer for OpenAiCompatibleSummarizer {
    async fn summarize(
        &self,
        transcript: &str,
        template: &SummarizeTemplate,
    ) -> Result<String, SummarizerError> {
        let mut headers = vec![("content-type".into(), "application/json".into())];
        if let Some(key) = &self.api_key {
            headers.push(("authorization".into(), format!("Bearer {key}")));
        }
        let body = serde_json::json!({"model": self.model, "messages": [{"role":"user","content":template.prompt(transcript)}]}).to_string();
        let response = self
            .transport
            .post(ChatRequest {
                url: format!("{}/v1/chat/completions", self.base_url),
                headers,
                body,
            })
            .await
            .map_err(SummarizerError::Transport)?;
        if !(200..300).contains(&response.status) {
            return Err(SummarizerError::Http(response.status));
        }
        let value: Value = serde_json::from_str(&response.body)
            .map_err(|e| SummarizerError::Malformed(e.to_string()))?;
        value
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| SummarizerError::Malformed("missing choices[0].message.content".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    struct Fake {
        response: ChatResponse,
        request: Mutex<Option<ChatRequest>>,
    }
    #[async_trait]
    impl ChatTransport for Fake {
        async fn post(&self, request: ChatRequest) -> Result<ChatResponse, String> {
            *self.request.lock().unwrap() = Some(request);
            Ok(self.response.clone())
        }
    }
    #[tokio::test]
    async fn prompt_preserves_and_discards_with_transcript() {
        let fake = Arc::new(Fake {
            response: ChatResponse {
                status: 200,
                body: r#"{"choices":[{"message":{"content":"summary"}}]}"#.into(),
            },
            request: Mutex::new(None),
        });
        let summarizer =
            OpenAiCompatibleSummarizer::new("http://local", "cheap", None, fake.clone());
        summarizer
            .summarize(
                "GOAL: ship it\nTOOL OUTPUT: already acted on",
                &SummarizeTemplate::default(),
            )
            .await
            .unwrap();
        let body = fake.request.lock().unwrap().as_ref().unwrap().body.clone();
        assert!(body.contains("GOAL: ship it"));
        assert!(body.contains("Preserve:"));
        assert!(body.contains("Discard: raw file contents"));
        assert!(body.contains("decisions already made and why"));
    }
    #[tokio::test]
    async fn parses_success_and_surfaces_bad_responses() {
        for (status, body, expected) in [
            (200, r#"{"choices":[{"message":{"content":"ok"}}]}"#, "ok"),
            (500, "oops", ""),
            (200, "{}", ""),
        ] {
            let fake = Arc::new(Fake {
                response: ChatResponse {
                    status,
                    body: body.into(),
                },
                request: Mutex::new(None),
            });
            let result =
                OpenAiCompatibleSummarizer::new("http://local", "cheap", Some("key".into()), fake)
                    .summarize("t", &SummarizeTemplate::default())
                    .await;
            if expected.is_empty() {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), expected);
            }
        }
    }
}
