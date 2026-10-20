import json

import pytest

from job_sl.runtime import BlockedByRemote, BlockGuard, Deadline, emit_summary


def test_block_guard_trips_when_window_all_fail():
    g = BlockGuard(window=5, min_ok_ratio=0.2)
    for _ in range(4):
        g.record(False)
    with pytest.raises(BlockedByRemote):
        g.record(False)


def test_block_guard_passes_when_some_ok_and_stops_judging_after_window():
    g = BlockGuard(window=5, min_ok_ratio=0.2)
    g.record(True)
    for _ in range(4):
        g.record(False)
    for _ in range(100):
        g.record(False)


def test_deadline():
    assert not Deadline(None).expired()
    assert not Deadline(1).expired()


def test_emit_summary_is_single_json_line(capsys):
    emit_summary("geekly1", "ok", fetched=3)
    line = capsys.readouterr().out.strip()
    d = json.loads(line)
    assert (
        d["event"] == "run_summary" and d["source"] == "geekly1" and d["fetched"] == 3
    )
