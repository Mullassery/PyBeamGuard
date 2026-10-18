"""AST inventory tests (no native bindings needed)."""

from pybeamguard.ast_inventory import inventory_source, render_text


def test_detects_unbounded_source_without_windowing():
    source = """
import apache_beam as beam
with beam.Pipeline() as p:
    (p | beam.io.ReadFromPubSub(topic="t")
       | beam.Map(lambda x: x)
       | beam.io.WriteToBigQuery("t"))
"""
    inventory = inventory_source(source)
    assert inventory["parsed"]
    assert "ReadFromPubSub" in inventory["sources"]
    assert "WriteToBigQuery" in inventory["sinks"]
    assert not inventory["has_windowing"]
    assert inventory["findings"][0]["id"] == "unbounded_source_without_window"


def test_windowing_suppresses_the_finding():
    source = """
import apache_beam as beam
with beam.Pipeline() as p:
    (p | beam.io.ReadFromKafka(...)
       | beam.WindowInto(beam.window.FixedWindows(60))
       | beam.io.WriteToBigQuery("t"))
"""
    inventory = inventory_source(source)
    assert inventory["has_windowing"]
    assert inventory["findings"] == []


def test_syntax_error_is_reported_not_raised():
    inventory = inventory_source("def broken(:\n")
    assert inventory["parsed"] is False
    assert inventory["syntax_error"]
    assert "parse failed" in render_text(inventory)


def test_flags_group_by_key_without_combiner():
    source = """
import apache_beam as beam
with beam.Pipeline() as p:
    (p | beam.Create([1, 2]) | beam.GroupByKey() | beam.io.WriteToText("out"))
"""
    inventory = inventory_source(source)
    assert any(f["id"] == "group_by_key_without_combine" for f in inventory["findings"])


def test_combine_per_key_suppresses_the_group_finding():
    source = """
import apache_beam as beam
with beam.Pipeline() as p:
    (p | beam.Create([(1, 2)]) | beam.CombinePerKey(sum) | beam.io.WriteToText("out"))
"""
    inventory = inventory_source(source)
    assert not any(f["id"] == "group_by_key_without_combine" for f in inventory["findings"])
