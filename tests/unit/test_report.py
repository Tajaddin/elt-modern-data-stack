import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "report_runs.py"
spec = importlib.util.spec_from_file_location("report_runs", SCRIPT)
assert spec and spec.loader
report_runs = importlib.util.module_from_spec(spec)
sys.modules["report_runs"] = report_runs
spec.loader.exec_module(report_runs)


def _row(run_date: str, rows: int, total: float) -> dict[str, object]:
    return {
        "run_date": run_date,
        "window_start": "2026-10-03",
        "window_end": run_date,
        "rows_extracted": rows,
        "rows_loaded": rows,
        "bytes_bronze": 1_878_326,
        "extract_seconds": 1.8,
        "load_seconds": 0.2,
        "total_seconds": total,
        "stations_loaded": 78568,
    }


def test_render_empty() -> None:
    assert report_runs.render([], 30) == "No runs recorded yet."


def test_render_table_and_medians() -> None:
    out = report_runs.render(
        [_row("2026-10-17", 659929, 9.9), _row("2026-10-18", 661000, 11.1), _row("2026-10-19", 660500, 10.0)],
        30,
    )
    assert (
        "| 2026-10-17 | 2026-10-03 to 2026-10-17 | 659,929 | 659,929 | 1.9 | 1.8 | 0.2 | 9.9 | 78568 |" in out
    )
    assert "Median rows processed per run: **660,500**" in out
    assert "Median end-to-end (extract + load + stations): **10.0 s**" in out
    assert "Runs recorded: 3 (showing the last 3)" in out


def test_render_limits_to_last_n_days() -> None:
    rows = [_row(f"2026-10-{d:02d}", 100 * d, float(d)) for d in range(1, 11)]
    out = report_runs.render(rows, 3)
    assert "Runs recorded: 10 (showing the last 3)" in out
    assert "| 2026-10-07 |" not in out
    assert "| 2026-10-08 |" in out
