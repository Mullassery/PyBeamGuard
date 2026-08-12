"""Command-line entry point for the ``pybeamguard`` console script.

This is what `pip install pybeamguard && pybeamguard ...` actually runs
(wired up via ``[project.scripts]`` in ``pyproject.toml``). It is a thin
Python wrapper around the compiled PyO3 extension module
(:mod:`pybeamguard.pybeamguard`, re-exported as ``pybeamguard.core``).

Note this is a *different* binary from ``crates/core/src/bin/pybeamguard.rs``,
the standalone Rust CLI distributed via GitHub Releases. That Rust binary has
no Python dependency at all; this module exists because the PyPI wheel only
ships the compiled extension module (built by maturin), not the separate
Rust binary, so a pure-Python entry point is required to give `pip install`
users a `pybeamguard` command. The two intentionally mirror each other's
flags and output so behavior is consistent regardless of how the tool was
installed.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional, Sequence

from . import __version__

_SEVERITY_ORDER = {
    "info": 1,
    "low": 2,
    "medium": 3,
    "high": 4,
    "critical": 5,
}


def _severity_rank(value: str) -> int:
    return _SEVERITY_ORDER.get(value.lower(), 0)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pybeamguard",
        description="Apache Beam & Dataflow pipeline analysis platform.",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show the installed pybeamguard version and exit.",
    )

    subparsers = parser.add_subparsers(dest="command")

    analyze_parser = subparsers.add_parser(
        "analyze", help="Analyze a Beam pipeline Python file."
    )
    analyze_parser.add_argument("pipeline", help="Path to a pipeline source .py file.")
    analyze_parser.add_argument(
        "--framework",
        choices=["beam", "flink", "spark"],
        default="beam",
        help="Pipeline framework: beam (default), flink, or spark.",
    )
    analyze_parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    analyze_parser.add_argument(
        "--data-profile",
        dest="data_profile",
        default=None,
        metavar="FILE",
        help="Path to a data profile JSON file (optional).",
    )
    analyze_parser.add_argument(
        "--fail-on",
        dest="fail_on",
        default=None,
        metavar="SEVERITY",
        choices=["info", "low", "medium", "high", "critical"],
        help=(
            "Exit with a non-zero status if any finding meets or exceeds "
            "this severity (info|low|medium|high|critical). Useful for "
            "gating CI builds."
        ),
    )

    return parser


def _cmd_analyze(args: argparse.Namespace) -> int:
    from . import core as native  # local import: keeps --help usable even if the native module failed to load

    if native is None:
        print(
            "Error: PyBeamGuard native bindings are not available in this install.",
            file=sys.stderr,
        )
        return 1

    try:
        with open(args.pipeline, "r", encoding="utf-8") as f:
            code = f.read()
    except OSError as e:
        print(f"Error reading pipeline file '{args.pipeline}': {e}", file=sys.stderr)
        return 1

    data_profile_json: Optional[str] = None
    if args.data_profile:
        try:
            with open(args.data_profile, "r", encoding="utf-8") as f:
                data_profile_json = f.read()
        except OSError as e:
            print(
                f"Error reading data profile file '{args.data_profile}': {e}",
                file=sys.stderr,
            )
            return 1
        try:
            json.loads(data_profile_json)
        except json.JSONDecodeError as e:
            print(f"Error parsing data profile JSON: {e}", file=sys.stderr)
            return 1

    json_report, text_report, structured = {
        "beam": (native.get_json_report, native.get_text_report, native.analyze_structured),
        "flink": (
            native.get_flink_json_report,
            native.get_flink_text_report,
            native.analyze_flink_structured,
        ),
        "spark": (
            native.get_spark_json_report,
            native.get_spark_text_report,
            native.analyze_spark_structured,
        ),
    }[args.framework]

    try:
        if args.format == "json":
            output = json_report(code, data_profile_json)
        else:
            output = text_report(code, data_profile_json)
    except Exception as e:  # native bindings raise plain Python exceptions
        print(f"Analysis failed: {e}", file=sys.stderr)
        return 1

    print(output)

    if args.fail_on:
        try:
            results = structured(code, data_profile_json)
        except Exception as e:
            print(f"Analysis failed: {e}", file=sys.stderr)
            return 1

        threshold = _severity_rank(args.fail_on)
        breached = any(
            _severity_rank(str(finding.severity)) >= threshold
            for result in results
            for finding in result.findings
        )
        if breached:
            print(
                f"\n--fail-on {args.fail_on}: at least one finding meets or exceeds this severity.",
                file=sys.stderr,
            )
            return 1

    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.version:
        print(f"pybeamguard {__version__}")
        return 0

    if args.command == "analyze":
        return _cmd_analyze(args)

    parser.print_help()
    return 0 if args.command is None else 1


if __name__ == "__main__":
    sys.exit(main())
