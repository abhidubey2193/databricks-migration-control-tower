from dataclasses import dataclass, field, asdict
from typing import Any

@dataclass
class Finding:
    limitation: str
    severity: str
    detail: str
    remediation: str

@dataclass
class Workload:
    workload_id: str
    kind: str
    name: str
    source_path: str = ""
    compute_mode: str = "unknown"
    task_count: int = 0
    status: str = "DISCOVERED"
    readiness: int = 100
    eligible: bool = True
    findings: list[Finding] = field(default_factory=list)
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self):
        return asdict(self)
