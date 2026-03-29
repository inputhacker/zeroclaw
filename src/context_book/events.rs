use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextBookEventEnvelope {
    #[serde(alias = "eventId")]
    pub event_id: String,
    #[serde(alias = "eventType")]
    pub event_type: String,
    #[serde(alias = "occurredAt")]
    pub occurred_at: String,
    #[serde(alias = "producerAgentId")]
    pub producer_agent_id: String,
    #[serde(alias = "entityId")]
    pub entity_id: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub meta: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedContextBookSseFrame {
    Event(ContextBookEventEnvelope),
    Heartbeat,
}

#[derive(Debug, Default)]
pub struct ContextBookSseParser {
    event_id: Option<String>,
    event_type: Option<String>,
    data_lines: Vec<String>,
}

impl ContextBookSseParser {
    pub fn push_line(&mut self, line: &str) -> anyhow::Result<Option<ParsedContextBookSseFrame>> {
        if line.starts_with(':') {
            return Ok(Some(ParsedContextBookSseFrame::Heartbeat));
        }

        if line.is_empty() {
            return self.finish_event();
        }

        if let Some(value) = line.strip_prefix("id:") {
            self.event_id = Some(value.trim().to_string());
            return Ok(None);
        }

        if let Some(value) = line.strip_prefix("event:") {
            self.event_type = Some(value.trim().to_string());
            return Ok(None);
        }

        if let Some(value) = line.strip_prefix("data:") {
            self.data_lines.push(value.trim_start().to_string());
        }

        Ok(None)
    }

    fn finish_event(&mut self) -> anyhow::Result<Option<ParsedContextBookSseFrame>> {
        if self.data_lines.is_empty() {
            self.reset();
            return Ok(None);
        }

        let payload = self.data_lines.join("\n");
        let mut envelope: ContextBookEventEnvelope = serde_json::from_str(&payload)?;

        if envelope.event_id.trim().is_empty() {
            envelope.event_id = self.event_id.clone().unwrap_or_default();
        }
        if envelope.event_type.trim().is_empty() {
            envelope.event_type = self.event_type.clone().unwrap_or_default();
        }

        let frame = if envelope.event_type.eq_ignore_ascii_case("heartbeat")
            || envelope.event_type.eq_ignore_ascii_case("keepalive")
            || envelope
                .event_type
                .eq_ignore_ascii_case("bootstrap.keepalive")
        {
            ParsedContextBookSseFrame::Heartbeat
        } else {
            ParsedContextBookSseFrame::Event(envelope)
        };

        self.reset();
        Ok(Some(frame))
    }

    fn reset(&mut self) {
        self.event_id = None;
        self.event_type = None;
        self.data_lines.clear();
    }
}

pub fn parse_polled_events(value: Value) -> anyhow::Result<Vec<ContextBookEventEnvelope>> {
    if value.is_array() {
        return Ok(serde_json::from_value(value)?);
    }

    if let Some(events) = value.get("events") {
        return Ok(serde_json::from_value(events.clone())?);
    }

    if let Some(events) = value.get("items") {
        return Ok(serde_json::from_value(events.clone())?);
    }

    anyhow::bail!("Context Book polling response did not contain an events array")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;

    #[test]
    fn parser_ignores_heartbeat_comments() {
        let mut parser = ContextBookSseParser::default();

        let frame = parser.push_line(": keepalive").expect("comment frame");

        assert_eq!(frame, Some(ParsedContextBookSseFrame::Heartbeat));
    }

    #[test]
    fn parser_builds_event_from_sse_frame() {
        let mut parser = ContextBookSseParser::default();
        parser.push_line("id: evt-001").unwrap();
        parser.push_line("event: context.created").unwrap();
        parser
            .push_line(
                r#"data: {"eventId":"","eventType":"","occurredAt":"2026-03-29T00:00:00Z","producerAgentId":"peer","entityId":"ctx-1","payload":{},"meta":{}}"#,
            )
            .unwrap();

        let frame = parser.push_line("").expect("dispatch frame");

        assert_eq!(
            frame,
            Some(ParsedContextBookSseFrame::Event(ContextBookEventEnvelope {
                event_id: "evt-001".into(),
                event_type: "context.created".into(),
                occurred_at: "2026-03-29T00:00:00Z".into(),
                producer_agent_id: "peer".into(),
                entity_id: "ctx-1".into(),
                payload: Value::Object(Map::default()),
                meta: Value::Object(Map::default()),
            }))
        );
    }
}
