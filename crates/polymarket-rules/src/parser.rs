use polymarket_core::{SemanticAttribute, SemanticValue, SemanticValueKind};

pub fn parse_semantic_attributes(title: &str, rules_text: &str) -> Vec<SemanticAttribute> {
    let mut attributes = Vec::new();
    let corpus = format!("{title}\n{rules_text}").to_ascii_lowercase();

    if let Some(deadline) = extract_deadline(&corpus) {
        attributes.push(SemanticAttribute {
            key: "deadline".to_owned(),
            value: SemanticValue {
                kind: SemanticValueKind::Timestamp,
                text: deadline,
            },
        });
    }

    if let Some(threshold) = extract_threshold(&corpus) {
        attributes.push(SemanticAttribute {
            key: "threshold".to_owned(),
            value: SemanticValue {
                kind: SemanticValueKind::Number,
                text: threshold,
            },
        });
    }

    if let Some(source) = extract_resolution_source(&corpus) {
        attributes.push(SemanticAttribute {
            key: "resolution_source".to_owned(),
            value: SemanticValue {
                kind: SemanticValueKind::Text,
                text: source,
            },
        });
    }

    attributes
}

fn extract_deadline(corpus: &str) -> Option<String> {
    for marker in [" by ", " before ", " on "] {
        if let Some(index) = corpus.find(marker) {
            let remainder = corpus[index + marker.len()..].trim();
            let candidate = remainder
                .split(|c: char| c == ',' || c == '.' || c == ';')
                .next()
                .unwrap_or(remainder)
                .trim();
            if !candidate.is_empty() {
                return Some(candidate.to_owned());
            }
        }
    }
    None
}

fn extract_threshold(corpus: &str) -> Option<String> {
    for token in corpus.split_whitespace() {
        if token.chars().any(|c| c.is_ascii_digit())
            && (token.contains('%') || token.contains('$') || token.contains('.'))
        {
            return Some(
                token
                    .trim_matches(|c: char| c == ',' || c == '.')
                    .to_owned(),
            );
        }
    }
    None
}

fn extract_resolution_source(corpus: &str) -> Option<String> {
    for marker in ["according to ", "resolution source:", "resolved using "] {
        if let Some(index) = corpus.find(marker) {
            let remainder = corpus[index + marker.len()..].trim();
            let candidate = remainder
                .split(|c: char| c == '\n' || c == '.' || c == ';')
                .next()
                .unwrap_or(remainder)
                .trim();
            if !candidate.is_empty() {
                return Some(candidate.to_owned());
            }
        }
    }
    None
}
