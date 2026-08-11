"""Python-level smoke tests for the PyBeamGuard PyO3 bindings and CLI.

These require the native extension module to be built and importable, i.e.
run `maturin develop` (or install a built wheel) into the active
environment before running pytest:

    python3 -m venv .venv && source .venv/bin/activate
    pip install maturin pytest
    maturin develop
    pytest tests/

This is the actual PyPI distribution path (a maturin-built wheel containing
the compiled `pybeamguard.pybeamguard` extension module plus the
`src/pybeamguard` Python shim), so these tests exercise what a real
`pip install pybeamguard` user gets -- not just the Rust `cargo test` suite.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

pybeamguard = pytest.importorskip("pybeamguard")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXAMPLE_PIPELINE = os.path.join(REPO_ROOT, "examples", "pipeline_simple.py")

SIMPLE_PIPELINE_SOURCE = """
import apache_beam as beam

with beam.Pipeline() as p:
    result = (
        p
        | 'Read' >> beam.io.ReadFromText('input.txt')
        | 'Parse' >> beam.ParDo(ParseFn())
        | 'GroupByCustomer' >> beam.GroupByKey()
        | 'CountPerCustomer' >> beam.CombinePerKey(sum)
        | 'Write' >> beam.io.WriteToText('output.txt')
    )
"""


def test_native_module_is_available():
    assert pybeamguard.core is not None, (
        "Native PyO3 extension failed to import -- run `maturin develop` "
        "before running this test suite."
    )


def test_analyze_structured_runs_all_analyzers():
    results = pybeamguard.core.analyze_structured(SIMPLE_PIPELINE_SOURCE)
    assert len(results) == 10
    names = {r.analyzer_name for r in results}
    assert "HotKeyAnalyzer" in names
    assert "CostAnalyzer" in names
    assert "SynthesisEngine" in names


def test_analyze_structured_with_data_profile_changes_output():
    profile = json.dumps(
        {
            "estimated_throughput_per_sec": 1000.0,
            "average_element_size_bytes": 512,
            "key_cardinality": 10,
            "estimated_state_size_gb": 1.0,
        }
    )
    results = pybeamguard.core.analyze_structured(SIMPLE_PIPELINE_SOURCE, profile)
    hotkey = next(r for r in results if r.analyzer_name == "HotKeyAnalyzer")
    # key_cardinality=10 is dangerously low -- the data-profile-aware path
    # in HotKeyAnalyzer should escalate to a critical finding regardless of
    # the key's name.
    assert any(
        f.id == "HOTKEY_LOW_CARDINALITY_MEASURED" for f in hotkey.findings
    )


def test_analyze_structured_rejects_malformed_data_profile():
    with pytest.raises(Exception):
        pybeamguard.core.analyze_structured(SIMPLE_PIPELINE_SOURCE, "{not valid json")


def test_analyze_structured_rejects_non_beam_source():
    with pytest.raises(Exception):
        pybeamguard.core.analyze_structured("print('no beam here')")


def test_analyze_structured_empty_source_raises_not_panics():
    with pytest.raises(Exception):
        pybeamguard.core.analyze_structured("")


def test_get_text_report_and_get_json_report():
    text = pybeamguard.core.get_text_report(SIMPLE_PIPELINE_SOURCE)
    assert "PyBeamGuard Analysis Report" in text

    raw_json = pybeamguard.core.get_json_report(SIMPLE_PIPELINE_SOURCE)
    parsed = json.loads(raw_json)
    assert isinstance(parsed, list)
    assert len(parsed) == 10


def test_example_pipeline_file_analyzes_successfully():
    with open(EXAMPLE_PIPELINE, "r", encoding="utf-8") as f:
        code = f.read()
    results = pybeamguard.core.analyze_structured(code)
    assert len(results) == 10


def test_python_shim_version_matches_installed_package_metadata():
    """Regression test: pybeamguard.__version__ used to be a hardcoded
    string ("1.0.0") in src/pybeamguard/__init__.py that silently drifted
    out of sync with the actual released version on every bump. It now
    derives from the installed package's metadata (the same source
    `pip show pybeamguard` reads), so this must always match what setuptools
    /maturin recorded for this install -- and, in particular, must not be
    the old hardcoded literal.
    """
    from importlib.metadata import version as pkg_version

    assert pybeamguard.__version__ == pkg_version("pybeamguard")
    assert pybeamguard.__version__ != "1.0.0"


class TestCliMain:
    """Exercises pybeamguard.cli.main() in-process (no subprocess)."""

    def test_version_flag(self, capsys):
        from pybeamguard.cli import main

        exit_code = main(["--version"])
        captured = capsys.readouterr()
        assert exit_code == 0
        assert "pybeamguard" in captured.out

    def test_help_with_no_args(self, capsys):
        from pybeamguard.cli import main

        exit_code = main([])
        captured = capsys.readouterr()
        assert exit_code == 0
        assert "usage" in captured.out.lower()

    def test_analyze_text_format(self, capsys):
        from pybeamguard.cli import main

        exit_code = main(["analyze", EXAMPLE_PIPELINE])
        captured = capsys.readouterr()
        assert exit_code == 0
        assert "PyBeamGuard Analysis Report" in captured.out

    def test_analyze_json_format(self, capsys):
        from pybeamguard.cli import main

        exit_code = main(["analyze", EXAMPLE_PIPELINE, "--format", "json"])
        captured = capsys.readouterr()
        assert exit_code == 0
        parsed = json.loads(captured.out)
        assert isinstance(parsed, list)

    def test_analyze_fail_on_triggers_nonzero_exit(self, capsys):
        from pybeamguard.cli import main

        # Every analyzer that fires at all will have at least an "info"
        # level finding somewhere, so --fail-on info should always trip.
        exit_code = main(["analyze", EXAMPLE_PIPELINE, "--fail-on", "info"])
        assert exit_code == 1

    def test_analyze_missing_file_is_clean_error(self, capsys):
        from pybeamguard.cli import main

        exit_code = main(["analyze", "/nonexistent/path/to/pipeline.py"])
        captured = capsys.readouterr()
        assert exit_code == 1
        assert "Error reading pipeline file" in captured.err

    def test_analyze_malformed_data_profile_is_clean_error(self, capsys, tmp_path):
        from pybeamguard.cli import main

        bad_profile = tmp_path / "bad_profile.json"
        bad_profile.write_text("{ not valid json")

        exit_code = main(
            ["analyze", EXAMPLE_PIPELINE, "--data-profile", str(bad_profile)]
        )
        captured = capsys.readouterr()
        assert exit_code == 1
        assert "Error parsing data profile JSON" in captured.err

    def test_analyze_empty_pipeline_file_is_clean_error(self, capsys, tmp_path):
        from pybeamguard.cli import main

        empty_file = tmp_path / "empty.py"
        empty_file.write_text("")

        exit_code = main(["analyze", str(empty_file)])
        captured = capsys.readouterr()
        assert exit_code == 1
        assert "Analysis failed" in captured.err


def test_console_script_entry_point_end_to_end():
    """Invokes the actual installed `pybeamguard` console script as a
    subprocess, the way a real end user would after `pip install
    pybeamguard`, rather than calling into cli.main() in-process."""
    result = subprocess.run(
        [sys.executable, "-m", "pybeamguard.cli", "--version"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "pybeamguard" in result.stdout
