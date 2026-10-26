"""Pure cutover safety gate; intentionally no trigger updates are performed."""
from dataclasses import dataclass

@dataclass
class Gate:
    allowed: bool
    reasons: list[str]

def evaluate_cutover(approved: bool, backup_exists: bool, ab_passed: bool,
                     clone_trigger_disabled: bool, repo_reconciled: bool) -> Gate:
    checks = {"Approval missing": approved,
              "Rollback backup missing": backup_exists,
              "A/B validation not passed": ab_passed,
              "Clone trigger is not disabled": clone_trigger_disabled,
              "Source bundle/Git reconciliation pending": repo_reconciled}
    reasons = [message for message, passed in checks.items() if not passed]
    return Gate(not reasons, reasons)
