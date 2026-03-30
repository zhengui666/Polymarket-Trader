use std::collections::BTreeSet;

use polymarket_core::ClarificationRecord;

pub fn normalize_clarifications(
    input: &[ClarificationRecord],
    max_keep: usize,
) -> Vec<ClarificationRecord> {
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();

    for item in input {
        let key = (item.occurred_at, item.text.trim().to_ascii_lowercase());
        if seen.insert(key) {
            output.push(item.clone());
        }
    }

    if output.len() > max_keep {
        output.split_off(output.len() - max_keep)
    } else {
        output
    }
}
