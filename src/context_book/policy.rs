use super::store::{ContextBookCachedItems, ContextBookContextSnapshot, ContextBookVoteSnapshot};
use serde::Serialize;
use std::collections::BTreeSet;

const MAX_POLICY_ITEMS: usize = 5;
const MIN_TOKEN_LEN: usize = 4;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextBookPolicyReadMode {
    Auto,
    Cache,
    Remote,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextBookPolicySource {
    Cache,
    Remote,
    Mixed,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextBookPolicyContextReview {
    pub context_id: String,
    pub author_agent_id: String,
    pub title: String,
    pub tag: String,
    pub updated_at: Option<String>,
    pub related_vote_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ContextBookPolicyVoteReview {
    pub vote_id: String,
    pub owner_agent_id: String,
    pub vote_score: f64,
    pub required_score: Option<i64>,
    pub executable: Option<bool>,
    pub vote_context: String,
    pub updated_at: Option<String>,
    pub related_context_ids: Vec<String>,
    pub missing_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextBookPolicyBlockedVote {
    pub vote_id: String,
    pub owner_agent_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ContextBookPolicyReference {
    pub agent_id: String,
    pub focus: Option<String>,
    pub source: ContextBookPolicySource,
    pub contexts_updated_at: Option<String>,
    pub votes_updated_at: Option<String>,
    pub review_contexts: Vec<ContextBookPolicyContextReview>,
    pub owned_votes: Vec<ContextBookPolicyVoteReview>,
    pub castable_votes: Vec<ContextBookPolicyVoteReview>,
    pub blocked_votes: Vec<ContextBookPolicyBlockedVote>,
    pub prompt_block: String,
}

impl ContextBookPolicyReference {
    pub fn has_actionable_items(&self) -> bool {
        !self.review_contexts.is_empty()
            || !self.owned_votes.is_empty()
            || !self.castable_votes.is_empty()
    }
}

pub fn build_policy_reference(
    agent_id: &str,
    focus: Option<&str>,
    source: ContextBookPolicySource,
    contexts: &ContextBookCachedItems<ContextBookContextSnapshot>,
    votes: &ContextBookCachedItems<ContextBookVoteSnapshot>,
) -> ContextBookPolicyReference {
    let focus = focus
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let focus_tokens = tokenize(focus.as_deref().unwrap_or_default());

    let mut review_contexts = contexts
        .items
        .iter()
        .filter(|context| context.status == "Published" && context.author_agent_id != agent_id)
        .map(|context| {
            let related_vote_ids = related_vote_ids_for_context(context, &votes.items);
            let score = context_relevance_score(context, &focus_tokens)
                + if related_vote_ids.is_empty() { 10 } else { 0 };
            (score, context.updated_at.clone(), context, related_vote_ids)
        })
        .filter(|(score, _, _, related_vote_ids)| *score > 0 || related_vote_ids.is_empty())
        .collect::<Vec<_>>();
    review_contexts
        .sort_by(|left, right| compare_ranked(left.0, left.1.as_ref(), right.0, right.1.as_ref()));
    let review_contexts = review_contexts
        .into_iter()
        .filter(|(_, _, _, related_vote_ids)| related_vote_ids.is_empty())
        .take(MAX_POLICY_ITEMS)
        .map(
            |(_, _, context, related_vote_ids)| ContextBookPolicyContextReview {
                context_id: context.context_id.clone(),
                author_agent_id: context.author_agent_id.clone(),
                title: context.title.clone(),
                tag: context.tag.clone(),
                updated_at: context.updated_at.clone(),
                related_vote_ids,
            },
        )
        .collect::<Vec<_>>();

    let mut owned_votes = Vec::new();
    let mut castable_votes = Vec::new();
    let mut blocked_votes = Vec::new();

    let mut ranked_votes = votes
        .items
        .iter()
        .map(|vote| {
            let related_context_ids = related_context_ids_for_vote(vote, &contexts.items);
            let score = vote_relevance_score(vote, &focus_tokens)
                + if related_context_ids.is_empty() { 0 } else { 5 };
            (score, vote.updated_at.clone(), vote, related_context_ids)
        })
        .collect::<Vec<_>>();
    ranked_votes
        .sort_by(|left, right| compare_ranked(left.0, left.1.as_ref(), right.0, right.1.as_ref()));

    for (_, _, vote, related_context_ids) in ranked_votes {
        let review = ContextBookPolicyVoteReview {
            vote_id: vote.vote_id.clone(),
            owner_agent_id: vote.owner_agent_id.clone(),
            vote_score: vote.vote_score,
            required_score: vote.required_score,
            executable: vote.executable,
            vote_context: vote.vote_context.clone(),
            updated_at: vote.updated_at.clone(),
            related_context_ids,
            missing_score: missing_score(vote),
        };

        if vote.owner_agent_id == agent_id {
            if vote.executable != Some(true) && owned_votes.len() < MAX_POLICY_ITEMS {
                owned_votes.push(review);
            } else if blocked_votes.len() < MAX_POLICY_ITEMS {
                blocked_votes.push(ContextBookPolicyBlockedVote {
                    vote_id: vote.vote_id.clone(),
                    owner_agent_id: vote.owner_agent_id.clone(),
                    reason: "owner_cannot_cast".to_string(),
                });
            }
            continue;
        }

        if vote.voter_agent_ids.iter().any(|voter| voter == agent_id) {
            if blocked_votes.len() < MAX_POLICY_ITEMS {
                blocked_votes.push(ContextBookPolicyBlockedVote {
                    vote_id: vote.vote_id.clone(),
                    owner_agent_id: vote.owner_agent_id.clone(),
                    reason: "already_cast".to_string(),
                });
            }
            continue;
        }

        if vote.executable != Some(true) && castable_votes.len() < MAX_POLICY_ITEMS {
            castable_votes.push(review);
        }
    }

    let mut reference = ContextBookPolicyReference {
        agent_id: agent_id.to_string(),
        focus,
        source,
        contexts_updated_at: contexts.updated_at.clone(),
        votes_updated_at: votes.updated_at.clone(),
        review_contexts,
        owned_votes,
        castable_votes,
        blocked_votes,
        prompt_block: String::new(),
    };
    reference.prompt_block = render_prompt_block(&reference);
    reference
}

fn render_prompt_block(reference: &ContextBookPolicyReference) -> String {
    let mut lines = vec![
        "[Context Book policy helper]".to_string(),
        format!("agent_id: {}", reference.agent_id),
        format!("source: {}", policy_source_label(reference.source)),
    ];
    if let Some(focus) = reference.focus.as_deref() {
        lines.push(format!("focus: {focus}"));
    }
    if !reference.review_contexts.is_empty() {
        lines.push("peer contexts needing vote review:".to_string());
        for context in &reference.review_contexts {
            lines.push(format!(
                "- {} | author={} | tag={} | title={}",
                context.context_id, context.author_agent_id, context.tag, context.title
            ));
        }
    }
    if !reference.owned_votes.is_empty() {
        lines.push("owned votes needing follow-up:".to_string());
        for vote in &reference.owned_votes {
            lines.push(render_vote_line(vote));
        }
    }
    if !reference.castable_votes.is_empty() {
        lines.push("cast opportunities:".to_string());
        for vote in &reference.castable_votes {
            lines.push(render_vote_line(vote));
        }
    }
    if !reference.blocked_votes.is_empty() {
        lines.push("blocked cast votes:".to_string());
        for vote in &reference.blocked_votes {
            lines.push(format!(
                "- {} | owner={} | reason={}",
                vote.vote_id, vote.owner_agent_id, vote.reason
            ));
        }
    }
    lines.join("\n")
}

fn render_vote_line(vote: &ContextBookPolicyVoteReview) -> String {
    let required = vote
        .required_score
        .map_or_else(|| "n/a".to_string(), |value| value.to_string());
    let executable = vote
        .executable
        .map_or_else(|| "unknown".to_string(), |value| value.to_string());
    let related = if vote.related_context_ids.is_empty() {
        "none".to_string()
    } else {
        vote.related_context_ids.join(",")
    };
    let gap = vote
        .missing_score
        .map_or_else(|| "0".to_string(), |value| format!("{value:.1}"));
    format!(
        "- {} | owner={} | score={:.1}/{} | executable={} | missing_score={} | related_contexts={} | context={}",
        vote.vote_id,
        vote.owner_agent_id,
        vote.vote_score,
        required,
        executable,
        gap,
        related,
        truncate_text(&vote.vote_context, 96),
    )
}

fn policy_source_label(source: ContextBookPolicySource) -> &'static str {
    match source {
        ContextBookPolicySource::Cache => "cache",
        ContextBookPolicySource::Remote => "remote",
        ContextBookPolicySource::Mixed => "mixed",
    }
}

fn missing_score(vote: &ContextBookVoteSnapshot) -> Option<f64> {
    vote.required_score
        .map(|required| (required as f64 - vote.vote_score).max(0.0))
        .filter(|value| *value > 0.0)
}

fn related_vote_ids_for_context(
    context: &ContextBookContextSnapshot,
    votes: &[ContextBookVoteSnapshot],
) -> Vec<String> {
    votes
        .iter()
        .filter(|vote| vote_matches_context(vote, context))
        .map(|vote| vote.vote_id.clone())
        .collect()
}

fn related_context_ids_for_vote(
    vote: &ContextBookVoteSnapshot,
    contexts: &[ContextBookContextSnapshot],
) -> Vec<String> {
    contexts
        .iter()
        .filter(|context| context_matches_vote(context, vote))
        .map(|context| context.context_id.clone())
        .collect()
}

fn vote_matches_context(
    vote: &ContextBookVoteSnapshot,
    context: &ContextBookContextSnapshot,
) -> bool {
    context_matches_vote(context, vote)
}

fn context_matches_vote(
    context: &ContextBookContextSnapshot,
    vote: &ContextBookVoteSnapshot,
) -> bool {
    let vote_text = normalize_text(&vote.vote_context);
    let exact_needles = [
        normalize_text(&context.context_id),
        normalize_text(&context.title),
        normalize_text(&context.tag),
    ];
    if exact_needles
        .iter()
        .any(|needle| !needle.is_empty() && vote_text.contains(needle))
    {
        return true;
    }

    let vote_tokens = tokenize(&vote.vote_context);
    let context_tokens = tokenize(&format!(
        "{} {} {}",
        context.title, context.tag, context.contents
    ));
    vote_tokens.intersection(&context_tokens).count() >= 2
}

fn context_relevance_score(
    context: &ContextBookContextSnapshot,
    focus_tokens: &BTreeSet<String>,
) -> usize {
    if focus_tokens.is_empty() {
        return 1;
    }
    let haystack = format!("{} {} {}", context.title, context.tag, context.contents);
    tokenize(&haystack).intersection(focus_tokens).count()
}

fn vote_relevance_score(vote: &ContextBookVoteSnapshot, focus_tokens: &BTreeSet<String>) -> usize {
    if focus_tokens.is_empty() {
        return 1;
    }
    tokenize(&vote.vote_context)
        .intersection(focus_tokens)
        .count()
}

fn compare_ranked(
    left_score: usize,
    left_updated_at: Option<&String>,
    right_score: usize,
    right_updated_at: Option<&String>,
) -> std::cmp::Ordering {
    right_score
        .cmp(&left_score)
        .then_with(|| right_updated_at.cmp(&left_updated_at))
}

fn tokenize(value: &str) -> BTreeSet<String> {
    value
        .split(|ch: char| !ch.is_alphanumeric())
        .map(str::trim)
        .filter(|token| token.len() >= MIN_TOKEN_LEN)
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

fn normalize_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_alphanumeric() { ch } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let truncated = value.chars().take(max_chars).collect::<String>();
    format!("{truncated}...")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn contexts() -> ContextBookCachedItems<ContextBookContextSnapshot> {
        ContextBookCachedItems {
            updated_at: Some("2026-03-29T00:00:10Z".into()),
            items: vec![
                ContextBookContextSnapshot {
                    context_id: "peer_ctx_launch".into(),
                    author_agent_id: "peer-a".into(),
                    title: "Launch Plan".into(),
                    contents: "Ship the launch checklist today".into(),
                    tag: "eng".into(),
                    status: "Published".into(),
                    created_at: Some("2026-03-29T00:00:00Z".into()),
                    updated_at: Some("2026-03-29T00:00:01Z".into()),
                    raw_json: json!({"contextId": "peer_ctx_launch"}),
                    synced_at: "2026-03-29T00:00:10Z".into(),
                },
                ContextBookContextSnapshot {
                    context_id: "workspace_ctx_done".into(),
                    author_agent_id: "workspace".into(),
                    title: "Internal Note".into(),
                    contents: "No vote needed".into(),
                    tag: "ops".into(),
                    status: "Published".into(),
                    created_at: Some("2026-03-29T00:00:00Z".into()),
                    updated_at: Some("2026-03-29T00:00:02Z".into()),
                    raw_json: json!({"contextId": "workspace_ctx_done"}),
                    synced_at: "2026-03-29T00:00:10Z".into(),
                },
            ],
        }
    }

    fn votes() -> ContextBookCachedItems<ContextBookVoteSnapshot> {
        ContextBookCachedItems {
            updated_at: Some("2026-03-29T00:00:11Z".into()),
            items: vec![
                ContextBookVoteSnapshot {
                    vote_id: "peer_vote_launch".into(),
                    owner_agent_id: "peer-a".into(),
                    vote_score: 1.0,
                    vote_context: "Approve launch plan from peer_ctx_launch".into(),
                    voter_agent_ids: vec!["peer-b".into()],
                    required_score: Some(2),
                    executable: Some(false),
                    created_at: Some("2026-03-29T00:00:00Z".into()),
                    updated_at: Some("2026-03-29T00:00:03Z".into()),
                    raw_json: json!({"voteId": "peer_vote_launch"}),
                    synced_at: "2026-03-29T00:00:11Z".into(),
                },
                ContextBookVoteSnapshot {
                    vote_id: "workspace_vote_pending".into(),
                    owner_agent_id: "workspace".into(),
                    vote_score: 1.0,
                    vote_context: "Approve launch plan".into(),
                    voter_agent_ids: vec!["workspace".into()],
                    required_score: Some(2),
                    executable: Some(false),
                    created_at: Some("2026-03-29T00:00:00Z".into()),
                    updated_at: Some("2026-03-29T00:00:04Z".into()),
                    raw_json: json!({"voteId": "workspace_vote_pending"}),
                    synced_at: "2026-03-29T00:00:11Z".into(),
                },
                ContextBookVoteSnapshot {
                    vote_id: "peer_vote_already_cast".into(),
                    owner_agent_id: "peer-c".into(),
                    vote_score: 2.0,
                    vote_context: "Routine approval".into(),
                    voter_agent_ids: vec!["workspace".into()],
                    required_score: Some(2),
                    executable: Some(true),
                    created_at: Some("2026-03-29T00:00:00Z".into()),
                    updated_at: Some("2026-03-29T00:00:05Z".into()),
                    raw_json: json!({"voteId": "peer_vote_already_cast"}),
                    synced_at: "2026-03-29T00:00:11Z".into(),
                },
            ],
        }
    }

    #[test]
    fn policy_reference_derives_review_and_cast_actions() {
        let reference = build_policy_reference(
            "workspace",
            Some("launch approval"),
            ContextBookPolicySource::Mixed,
            &contexts(),
            &votes(),
        );

        assert_eq!(reference.agent_id, "workspace");
        assert_eq!(reference.source, ContextBookPolicySource::Mixed);
        assert_eq!(reference.review_contexts.len(), 0);
        assert_eq!(reference.owned_votes.len(), 1);
        assert_eq!(reference.castable_votes.len(), 1);
        assert_eq!(reference.blocked_votes.len(), 1);
        assert_eq!(
            reference.castable_votes[0].related_context_ids,
            vec!["peer_ctx_launch"]
        );
        assert!(reference.prompt_block.contains("cast opportunities"));
        assert!(
            reference
                .prompt_block
                .contains("owned votes needing follow-up")
        );
    }

    #[test]
    fn policy_reference_lists_unmatched_peer_contexts_for_review() {
        let mut context_items = contexts();
        context_items.items[0].title = "Security Audit".into();
        context_items.items[0].contents = "Check production secrets".into();
        context_items.items[0].tag = "ops".into();

        let reference = build_policy_reference(
            "workspace",
            Some("security"),
            ContextBookPolicySource::Cache,
            &context_items,
            &ContextBookCachedItems {
                items: Vec::new(),
                updated_at: None,
            },
        );

        assert_eq!(reference.review_contexts.len(), 1);
        assert_eq!(reference.review_contexts[0].context_id, "peer_ctx_launch");
        assert!(reference.has_actionable_items());
    }
}
