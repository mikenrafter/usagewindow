//! Provider-neutral compaction message construction.

/// The shared instruction body used when asking a harness to compact context.
pub fn instruction_body(reason: &str) -> String {
    format!(
        "Preserve: the current goal, the step in progress and its exact next action, decisions already made and why, and every approach already tried and rejected.\nDiscard: file contents already read, superseded plans, and tool output that has been acted on.\nReason for compacting now: {}",
        reason.trim()
    )
}

/// Formats a provider's compaction command and shared instructions.
///
/// The request may already contain one or more copies of the command. This is
/// intentional: queued requests can pass through more than one delivery layer,
/// and formatting must remain idempotent at each layer.
pub fn message(command: &str, instructions: &str) -> String {
    let command = command.trim();
    if command.is_empty() {
        return instructions.trim().to_owned();
    }
    let mut body = instructions.trim();
    while let Some(rest) = body.strip_prefix(command) {
        if rest.is_empty() || rest.chars().next().is_some_and(char::is_whitespace) {
            body = rest.trim_start();
        } else {
            break;
        }
    }
    if body.is_empty() {
        command.to_owned()
    } else {
        format!("{command} {body}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_the_shared_instruction_body_without_a_provider_command() {
        assert_eq!(
            instruction_body("idle cache expiry"),
            "Preserve: the current goal, the step in progress and its exact next action, decisions already made and why, and every approach already tried and rejected.\nDiscard: file contents already read, superseded plans, and tool output that has been acted on.\nReason for compacting now: idle cache expiry"
        );
    }

    #[test]
    fn prefixes_a_command_once_even_when_the_request_already_contains_it() {
        let body = instruction_body("idle cache expiry");

        assert_eq!(
            message("/compact", &format!("/compact\n{body}")),
            format!("/compact {body}")
        );
        assert_eq!(
            message("/compress", &format!("/compress /compress\n{body}")),
            format!("/compress {body}")
        );
    }

    #[test]
    fn supports_commandless_requests_and_empty_bodies() {
        assert_eq!(message("/compact", "instructions"), "/compact instructions");
        assert_eq!(message("/compact", ""), "/compact");
    }
}
