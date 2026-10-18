"""Python AST inventory for Beam pipelines.

The native analyzers are intentionally fast regex/line-based scans. This module
adds a small, optional AST pass that can see actual call structure: which
sources/sinks are used, whether any windowing transform appears, and whether an
unbounded source (Pub/Sub, Kafka) is used without a window. It does not try to
replace the native analyzers.
"""

from __future__ import annotations

import ast
from typing import Any, Dict, List

_UNBOUNDED_SOURCES = {
    "ReadFromPubSub",
    "ReadFromKafka",
    "ReadFromPubsub",
}
_WINDOWING = {
    "WindowInto",
    "FixedWindows",
    "SlidingWindows",
    "Sessions",
}
_SINKS = {
    "WriteToPubSub",
    "WriteToKafka",
    "WriteToBigQuery",
    "WriteToText",
    "WriteToParquet",
}


def _callee(node: ast.AST) -> str:
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Call):
        return _callee(node.func)
    return ""


def inventory_source(source: str) -> Dict[str, Any]:
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return {
            "parsed": False,
            "syntax_error": f"{exc.msg} (line {exc.lineno})",
            "transforms": [],
            "sources": [],
            "sinks": [],
            "has_windowing": False,
            "findings": [],
        }

    transforms: List[str] = []
    sources: List[str] = []
    sinks: List[str] = []
    has_windowing = False
    findings: List[Dict[str, str]] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _callee(node)
        if not name:
            continue
        transforms.append(name)
        if name in _UNBOUNDED_SOURCES:
            sources.append(name)
        if name in _SINKS:
            sinks.append(name)
        if name in _WINDOWING:
            has_windowing = True

    if sources and not has_windowing:
        findings.append(
            {
                "id": "unbounded_source_without_window",
                "severity": "high",
                "message": "Unbounded source (%s) has no WindowInto/FixedWindows/SlidingWindows/Sessions transform in this file."
                % ", ".join(sorted(set(sources))),
            }
        )

    return {
        "parsed": True,
        "syntax_error": None,
        "transforms": sorted(set(transforms)),
        "sources": sorted(set(sources)),
        "sinks": sorted(set(sinks)),
        "has_windowing": has_windowing,
        "findings": findings,
    }


def render_text(inventory: Dict[str, Any]) -> str:
    if not inventory.get("parsed"):
        return "AST: parse failed: %s" % inventory.get("syntax_error")
    lines = [
        "AST inventory:",
        "  transforms: %s" % (", ".join(inventory["transforms"]) or "(none)"),
        "  sources: %s" % (", ".join(inventory["sources"]) or "(none)"),
        "  sinks: %s" % (", ".join(inventory["sinks"]) or "(none)"),
        "  windowing: %s" % ("yes" if inventory["has_windowing"] else "no"),
    ]
    for finding in inventory["findings"]:
        lines.append("  ! [%s] %s" % (finding["severity"], finding["message"]))
    return "\n".join(lines)
