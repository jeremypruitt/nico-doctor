//! PRD-007 Slice 1 (#372): the per-surface entity extraction primitive.
//!
//! `extract_entities(text, ctx) -> Vec<EntityRef>` is the single function
//! every correlate-drill trigger funnels its source text through. It
//! returns zero, one, or many `EntityRef`s depending on what the
//! [`ExtractionContext`] says about the surface the text came from, and
//! tags each result with the appropriate [`Confidence`] level
//! (Explicit / Parsed / Heuristic — see [`crate::model::Confidence`]).
//!
//! All four surfaces share the same identity vocabulary
//! (`nico_correlate::id::detect_id_type`) so doctor / correlate / ops
//! agree on what an entity ID looks like.

use nico_correlate::id::detect_id_type;

use crate::model::{Confidence, EntityRef};

/// Which surface the text came from. Drives both the extraction strategy
/// (single-token vs. multi-match scan vs. tag-first lookup) and the
/// confidence label attached to each returned `EntityRef`.
#[derive(Debug, Clone, Copy)]
pub enum ExtractionContext<'a> {
    /// Spotlight row — the surface yields an explicit id token; the
    /// caller has already isolated it. Single match, `Explicit`
    /// confidence.
    Spotlight,
    /// Findings detail row carrying a `next_command` CLI string
    /// (e.g. `"nico doctor hbn dpu-r12u5"`). Parse the trailing arg.
    /// At most one match, `Parsed` confidence.
    NextCommand,
    /// Raw log line — strict regex scan with word-boundary discipline.
    /// Zero, one, or many matches, all `Heuristic` confidence. URL
    /// paths (`https://example.com/dpu-foo`) must not match; bracketed
    /// mentions (`[dpu-r12u5]`) must.
    LogLine,
    /// Event timeline row — prefer event tags (`host_id`, `dpu_id`,
    /// `workflow_id`, `request_id`) when present; fall back to a log-line
    /// regex scan over `text` otherwise. Tagged → `Explicit`,
    /// untagged → `Heuristic`.
    EventRow { tags: &'a [(&'a str, &'a str)] },
}

/// Pull every entity (host / DPU / workflow / request) the given surface
/// can identify out of `text`. See [`ExtractionContext`] for the
/// per-surface contract.
pub fn extract_entities(text: &str, ctx: ExtractionContext<'_>) -> Vec<EntityRef> {
    match ctx {
        ExtractionContext::Spotlight => extract_spotlight(text),
        ExtractionContext::NextCommand => extract_next_command(text),
        ExtractionContext::LogLine => extract_log_line(text),
        ExtractionContext::EventRow { tags } => extract_event_row(text, tags),
    }
}

fn extract_spotlight(text: &str) -> Vec<EntityRef> {
    let trimmed = trim_outer_punct(text);
    match detect_id_type(trimmed) {
        Some(id_type) => vec![EntityRef {
            id: trimmed.to_string(),
            id_type,
            confidence: Confidence::Explicit,
        }],
        None => Vec::new(),
    }
}

fn extract_next_command(text: &str) -> Vec<EntityRef> {
    // `next_command` strings are CLI-style ("nico doctor <axis> <id>").
    // Walk tokens right-to-left and take the first one that detects as
    // an entity; this tolerates trailing flag-free arguments without
    // committing to a specific axis vocabulary.
    text.split_whitespace()
        .rev()
        .find_map(|raw| {
            let tok = trim_outer_punct(raw);
            detect_id_type(tok).map(|id_type| EntityRef {
                id: tok.to_string(),
                id_type,
                confidence: Confidence::Parsed,
            })
        })
        .into_iter()
        .collect()
}

fn extract_log_line(text: &str) -> Vec<EntityRef> {
    log_line_matches(text)
        .into_iter()
        .map(|(id, id_type)| EntityRef {
            id,
            id_type,
            confidence: Confidence::Heuristic,
        })
        .collect()
}

fn extract_event_row(text: &str, tags: &[(&str, &str)]) -> Vec<EntityRef> {
    if let Some(entity) = tag_lookup(tags) {
        return vec![entity];
    }
    extract_log_line(text)
}

/// Tag lookup vocabulary mirrors `IdType::label_key()` so doctor /
/// correlate / ops use one source of truth for tag names. Returns the
/// first tag that matches a known label key.
fn tag_lookup(tags: &[(&str, &str)]) -> Option<EntityRef> {
    use nico_correlate::id::IdType;
    let table = [
        ("host_id", IdType::Host),
        ("dpu_id", IdType::Dpu),
        ("workflow_id", IdType::Workflow),
        ("request_id", IdType::Request),
    ];
    for (label, id_type) in table {
        if let Some((_, value)) = tags.iter().find(|(k, _)| *k == label)
            && !value.is_empty()
        {
            return Some(EntityRef {
                id: (*value).to_string(),
                id_type,
                confidence: Confidence::Explicit,
            });
        }
    }
    None
}

/// Scan `text` for every entity ID, respecting strict word-boundary
/// discipline so URL-path substrings (preceded by `/`, `.`, or
/// alphanumerics) do not match. Whitespace, brackets, parens, quotes,
/// `=`, `,`, `:`, `;`, `<` are valid neighbours.
fn log_line_matches(text: &str) -> Vec<(String, nico_correlate::id::IdType)> {
    let bytes = text.as_bytes();
    let mut out: Vec<(String, nico_correlate::id::IdType)> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let prev_ok = i == 0 || is_left_boundary(bytes[i - 1]);
        if !prev_ok {
            i += 1;
            continue;
        }
        // Scan the longest id-shaped token starting at `i`.
        let mut j = i;
        while j < bytes.len() && is_id_char(bytes[j]) {
            j += 1;
        }
        if j > i {
            let next_ok = j == bytes.len() || is_right_boundary(bytes[j]);
            if next_ok {
                let tok = &text[i..j];
                if let Some(id_type) = detect_id_type(tok) {
                    out.push((tok.to_string(), id_type));
                }
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

fn is_id_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'_'
}

/// Chars that may sit immediately before an entity id: whitespace,
/// bracket/paren/quote openers, and a small set of separators. Crucially
/// excludes `/` (URL path) and `.` (hostnames, version strings) so those
/// surfaces do not produce false positives.
fn is_left_boundary(b: u8) -> bool {
    matches!(
        b,
        b' ' | b'\t' | b'\n' | b'\r' |
        b'[' | b'(' | b'<' | b'{' |
        b'"' | b'\'' | b'`' |
        b',' | b';' | b':' | b'=' | b'|'
    )
}

/// Symmetric to `is_left_boundary`: chars that may sit immediately
/// after an entity id without invalidating the match.
fn is_right_boundary(b: u8) -> bool {
    matches!(
        b,
        b' ' | b'\t' | b'\n' | b'\r' |
        b']' | b')' | b'>' | b'}' |
        b'"' | b'\'' | b'`' |
        b',' | b';' | b':' | b'.' | b'!' | b'?' | b'|'
    )
}

fn trim_outer_punct(s: &str) -> &str {
    s.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use nico_correlate::id::IdType;

    // ── Spotlight surface (Explicit) ────────────────────────────────────

    #[test]
    fn spotlight_explicit_host_returns_single_entity() {
        let out = extract_entities("host-r12u5", ExtractionContext::Spotlight);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "host-r12u5");
        assert_eq!(out[0].id_type, IdType::Host);
        assert_eq!(out[0].confidence, Confidence::Explicit);
    }

    #[test]
    fn spotlight_extracts_dpu() {
        let out = extract_entities("dpu-bf3-r12u5", ExtractionContext::Spotlight);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "dpu-bf3-r12u5");
        assert_eq!(out[0].id_type, IdType::Dpu);
        assert_eq!(out[0].confidence, Confidence::Explicit);
    }

    #[test]
    fn spotlight_extracts_workflow_with_hp_or_wf_prefix() {
        for id in ["hp-7f3a", "wf-001"] {
            let out = extract_entities(id, ExtractionContext::Spotlight);
            assert_eq!(out.len(), 1, "id={id}");
            assert_eq!(out[0].id_type, IdType::Workflow);
            assert_eq!(out[0].confidence, Confidence::Explicit);
        }
    }

    #[test]
    fn spotlight_extracts_request() {
        let out = extract_entities("req-a83b", ExtractionContext::Spotlight);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id_type, IdType::Request);
    }

    #[test]
    fn spotlight_extracts_bare_carbide_machine_id_as_host() {
        let id = "01HXP1ABCDEFGHJKMNPQRSTVWXYZ0123456789ABCDEFGHJKMNPQRSTVWX";
        let out = extract_entities(id, ExtractionContext::Spotlight);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id_type, IdType::Host);
    }

    #[test]
    fn spotlight_strips_surrounding_punctuation() {
        let out = extract_entities("[dpu-r12u5]", ExtractionContext::Spotlight);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "dpu-r12u5");
    }

    #[test]
    fn spotlight_with_unknown_token_returns_empty() {
        assert!(extract_entities("unknown-xyz", ExtractionContext::Spotlight).is_empty());
    }

    #[test]
    fn spotlight_with_empty_text_returns_empty() {
        assert!(extract_entities("", ExtractionContext::Spotlight).is_empty());
    }

    // ── NextCommand surface (Parsed) ────────────────────────────────────

    #[test]
    fn next_command_parses_trailing_dpu_arg() {
        let out = extract_entities("nico doctor hbn dpu-r12u5", ExtractionContext::NextCommand);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "dpu-r12u5");
        assert_eq!(out[0].id_type, IdType::Dpu);
        assert_eq!(out[0].confidence, Confidence::Parsed);
    }

    #[test]
    fn next_command_parses_trailing_host_arg() {
        let out = extract_entities("nico correlate host-r12u5", ExtractionContext::NextCommand);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "host-r12u5");
        assert_eq!(out[0].confidence, Confidence::Parsed);
    }

    #[test]
    fn next_command_returns_empty_when_no_token_matches() {
        let out = extract_entities("nico doctor --json", ExtractionContext::NextCommand);
        assert!(out.is_empty());
    }

    #[test]
    fn next_command_with_empty_text_returns_empty() {
        assert!(extract_entities("", ExtractionContext::NextCommand).is_empty());
    }

    // ── LogLine surface (Heuristic, multi-match) ────────────────────────

    #[test]
    fn log_line_extracts_single_entity_heuristic() {
        let out = extract_entities("provisioning dpu-r12u5 failed", ExtractionContext::LogLine);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "dpu-r12u5");
        assert_eq!(out[0].confidence, Confidence::Heuristic);
    }

    #[test]
    fn log_line_multi_match_returns_both_entities() {
        let out = extract_entities(
            "host-r12u5 had dpu-bf3-r12u5 disconnect",
            ExtractionContext::LogLine,
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].id, "host-r12u5");
        assert_eq!(out[0].id_type, IdType::Host);
        assert_eq!(out[1].id, "dpu-bf3-r12u5");
        assert_eq!(out[1].id_type, IdType::Dpu);
    }

    #[test]
    fn log_line_rejects_url_path_with_slash() {
        let out = extract_entities(
            "user clicked https://example.com/dpu-foo link",
            ExtractionContext::LogLine,
        );
        assert!(out.is_empty(), "URL path should not match: {out:?}");
    }

    #[test]
    fn log_line_accepts_bracketed_mention() {
        let out = extract_entities("incident: [dpu-r12u5] flapping", ExtractionContext::LogLine);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "dpu-r12u5");
    }

    #[test]
    fn log_line_handles_all_prefixes() {
        let out = extract_entities(
            "host-h1 dpu-d2 wf-w3 hp-w4 req-r5",
            ExtractionContext::LogLine,
        );
        let ids: Vec<&str> = out.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, vec!["host-h1", "dpu-d2", "wf-w3", "hp-w4", "req-r5"]);
        assert_eq!(out[0].id_type, IdType::Host);
        assert_eq!(out[1].id_type, IdType::Dpu);
        assert_eq!(out[2].id_type, IdType::Workflow);
        assert_eq!(out[3].id_type, IdType::Workflow);
        assert_eq!(out[4].id_type, IdType::Request);
    }

    #[test]
    fn log_line_extracts_bare_carbide_machine_id_as_host() {
        let id = "01HXP1ABCDEFGHJKMNPQRSTVWXYZ0123456789ABCDEFGHJKMNPQRSTVWX";
        let text = format!("error from {id} at 14:32");
        let out = extract_entities(&text, ExtractionContext::LogLine);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, id);
        assert_eq!(out[0].id_type, IdType::Host);
    }

    #[test]
    fn log_line_with_empty_text_returns_empty() {
        assert!(extract_entities("", ExtractionContext::LogLine).is_empty());
    }

    #[test]
    fn log_line_handles_comma_separated_ids() {
        let out = extract_entities("affected: host-a, dpu-b", ExtractionContext::LogLine);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].id, "host-a");
        assert_eq!(out[1].id, "dpu-b");
    }

    // ── EventRow surface (Explicit when tagged, Heuristic when not) ─────

    #[test]
    fn event_row_uses_host_id_tag_when_present() {
        let tags = [("host_id", "host-r12u5")];
        let out = extract_entities(
            "ignored message",
            ExtractionContext::EventRow { tags: &tags },
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "host-r12u5");
        assert_eq!(out[0].id_type, IdType::Host);
        assert_eq!(out[0].confidence, Confidence::Explicit);
    }

    #[test]
    fn event_row_uses_dpu_id_tag_when_present() {
        let tags = [("dpu_id", "dpu-bf3-r12u5")];
        let out = extract_entities("ignored", ExtractionContext::EventRow { tags: &tags });
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "dpu-bf3-r12u5");
        assert_eq!(out[0].id_type, IdType::Dpu);
        assert_eq!(out[0].confidence, Confidence::Explicit);
    }

    #[test]
    fn event_row_falls_back_to_message_regex_when_no_matching_tag() {
        let tags: [(&str, &str); 0] = [];
        let out = extract_entities(
            "stuck workflow hp-7f3a at 14:32",
            ExtractionContext::EventRow { tags: &tags },
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "hp-7f3a");
        assert_eq!(out[0].confidence, Confidence::Heuristic);
    }

    #[test]
    fn event_row_ignores_empty_tag_values() {
        let tags = [("host_id", "")];
        let out = extract_entities(
            "host-r12u5 fell off",
            ExtractionContext::EventRow { tags: &tags },
        );
        // Empty tag value → fall back to message regex.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "host-r12u5");
        assert_eq!(out[0].confidence, Confidence::Heuristic);
    }

    #[test]
    fn event_row_with_empty_text_and_no_tags_returns_empty() {
        let tags: [(&str, &str); 0] = [];
        assert!(extract_entities("", ExtractionContext::EventRow { tags: &tags }).is_empty());
    }
}
