"""Directory discovery tests for the Python CLI."""

from pybeamguard.cli import _pipeline_files


def test_single_file_and_directory_discovery(tmp_path):
    single = tmp_path / "pipeline.py"
    single.write_text("import apache_beam as beam\n", encoding="utf-8")
    assert _pipeline_files(str(single)) == [str(single)]

    nested = tmp_path / "pipelines"
    (nested / "__pycache__").mkdir(parents=True)
    (nested / "a.py").write_text("x = 1\n", encoding="utf-8")
    (nested / "b.py").write_text("y = 2\n", encoding="utf-8")
    (nested / "__pycache__" / "ignored.py").write_text("z = 3\n", encoding="utf-8")
    assert _pipeline_files(str(nested)) == [str(nested / "a.py"), str(nested / "b.py")]

    assert _pipeline_files(str(tmp_path / "missing")) == []
