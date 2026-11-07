"""Explicit operational redactions for immutable publication exports."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from artisan.schemas.enums import TablePath
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.replay import ReplaySnapshot


@dataclass(frozen=True)
class PublicationPolicy:
    """Match explicit private values without changing scientific semantics."""

    redact: tuple[str, ...]

    @classmethod
    def create(cls, values: Sequence[str]) -> PublicationPolicy:
        """Reject empty needles and ambiguous single-string policies."""
        if isinstance(values, str) or any(
            not isinstance(v, str) or not v for v in values
        ):
            msg = "redact must contain nonempty strings"
            raise ValueError(msg)
        return cls(tuple(sorted(set(values), key=len, reverse=True)))

    def matches(self, value: object) -> bool:
        """Check exact substrings, including binary scientific payloads."""
        if isinstance(value, str):
            if any(needle in value for needle in self.redact):
                return True
            if value.lstrip()[:1] in {"{", "[", '"'}:
                try:
                    decoded = json.loads(value)
                except json.JSONDecodeError:
                    return False
                return self.matches(decoded)
            return False
        if isinstance(value, bytes):
            if any(needle.encode() in value for needle in self.redact):
                return True
            if value.lstrip()[:1] in {b"{", b"[", b'"'}:
                try:
                    return self.matches(value.decode())
                except UnicodeDecodeError:
                    return False
            return False
        if isinstance(value, dict):
            return any(
                self.matches(key) or self.matches(item) for key, item in value.items()
            )
        if isinstance(value, (list, tuple)):
            return any(self.matches(item) for item in value)
        return False

    def protected(self, value: object, field: str) -> None:
        """Fail without echoing the private value from protected evidence."""
        if self.matches(value):
            msg = f"Publication redaction overlaps protected evidence: {field}"
            raise ValueError(msg)

    def text(self, value: str) -> str:
        """Redact a diagnostic string whose value does not define identity."""
        for needle in self.redact:
            value = value.replace(needle, "[redacted]")
        return value

    def diagnostic(self, value: Any) -> Any:
        """Preserve JSON structure and keys while redacting diagnostic values."""
        if isinstance(value, str):
            return self.text(value)
        if isinstance(value, dict):
            self.protected(list(value), "diagnostic keys")
            return {key: self.diagnostic(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.diagnostic(item) for item in value]
        return value

    def row(
        self, table: str, row: dict[str, Any], locations: dict[str, str]
    ) -> dict[str, Any]:
        """Transform only explicitly classified operational fields."""
        result = dict(row)
        if table == TablePath.ARTIFACT_LOCATIONS:
            result["uri"] = locations[row["artifact_id"]]
        elif table == TablePath.EXECUTIONS:
            self._execution(result)
        elif table == TablePath.STEPS:
            self._step(result)
        self.protected(result, table)
        return result

    def _execution(self, row: dict[str, Any]) -> None:
        for field in ("error", "tool_output", "worker_log"):
            if row[field] is not None:
                row[field] = self.text(row[field])
        for field in ("error_envelope", "command_recording"):
            if row[field] is not None:
                payload = self.diagnostic(json.loads(row[field]))
                if field == "command_recording":
                    CommandRecording.model_validate(payload)
                row[field] = json.dumps(payload, separators=(",", ":"))
        row["replay_snapshot"] = self.replay(row["replay_snapshot"])

    def _step(self, row: dict[str, Any]) -> None:
        if row["error"] is not None:
            row["error"] = self.text(row["error"])
        inputs = json.loads(row["input_refs_json"])
        entries = inputs.values() if isinstance(inputs, dict) else inputs or []
        for entry in entries:
            if isinstance(entry, dict) and entry.get("type") == "literal":
                entry["value"] = self.diagnostic(entry["value"])
        row["input_refs_json"] = json.dumps(inputs, separators=(",", ":"))

    def replay(self, value: str) -> str:
        """Omit config values with explicit replay replacement obligations."""
        snapshot = ReplaySnapshot.model_validate_json(value).model_dump(mode="json")
        operation = snapshot["operation"]
        if operation is not None:
            pointers = self._omit(operation["configuration"])
            snapshot["required_replacements"].extend(
                {"pointer": pointer, "suggested_environment": None}
                for pointer in pointers
            )
        for name in ("source", "diagnostic"):
            if snapshot[name] is not None:
                roots = snapshot[name]["roots"]
                for key, root in roots.items():
                    if self.matches(root):
                        roots[key] = None
        if snapshot["diagnostic"] is not None:
            for name in ("errors", "reproducibility_notes"):
                snapshot["diagnostic"][name] = self.diagnostic(
                    snapshot["diagnostic"][name]
                )
        return ReplaySnapshot.model_validate(snapshot).model_dump_json()

    def _omit(self, value: dict[str, Any] | list[Any], pointer: str = "") -> list[str]:
        pointers = []
        entries = value.items() if isinstance(value, dict) else enumerate(value)
        for key, item in entries:
            self.protected(key, "replay configuration key")
            child = pointer + "/" + str(key).replace("~", "~0").replace("/", "~1")
            if isinstance(item, str) and self.matches(item):
                value[key] = None
                pointers.append(child)
            elif isinstance(item, (dict, list)):
                pointers.extend(self._omit(item, child))
        return pointers
