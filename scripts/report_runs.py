"""Render the README's results table from committed run metrics.

    uv run python scripts/report_runs.py [--metrics data/metrics/extract_runs.jsonl] [--days 30]

Reads one JSON line per run (written by `ghcn-elt run` and by the Airflow DAG), prints a Markdown
table of the most recent runs and summary numbers (median rows per run, median end-to-end seconds,
run-success count). The README copies this output verbatim, with the machine it ran on stated.
"""

from __future__ import annotations

import argparse
import statistics
from pathlib import Path

from ghcn_elt.metrics import read_metrics


def render(rows: list[dict[str, object]], days: int) -> str:
    if not rows:
        return "No runs recorded yet."
    recent = rows[-days:]
    header = (
        "| run date | window | rows extracted | rows loaded | bronze MB "
        "| extract s | load s | total s | stations |"
    )
    lines = [
        f"Runs recorded: {len(rows)} (showing the last {len(recent)})",
        "",
        header,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in recent:
        cells = [
            str(r["run_date"]),
            f"{r['window_start']} to {r['window_end']}",
            f"{int(r['rows_extracted']):,}",
            f"{int(r['rows_loaded']):,}",
            f"{int(r['bytes_bronze']) / 1e6:.1f}",
            f"{float(r['extract_seconds']):.1f}",
            f"{float(r['load_seconds']):.1f}",
            f"{float(r['total_seconds']):.1f}",
            str(r.get("stations_loaded") or ""),
        ]
        lines.append("| " + " | ".join(cells) + " |")
    med_rows = statistics.median(int(r["rows_extracted"]) for r in recent)
    med_total = statistics.median(float(r["total_seconds"]) for r in recent)
    med_extract = statistics.median(float(r["extract_seconds"]) for r in recent)
    lines += [
        "",
        f"Median rows processed per run: **{med_rows:,.0f}**. Median extract time: **{med_extract:.1f} s**. "
        f"Median end-to-end (extract + load + stations): **{med_total:.1f} s**.",
    ]
    return "\n".join(lines)


START, END = "<!-- results:start -->", "<!-- results:end -->"


def splice_readme(readme: Path, table: str) -> None:
    """Replace the block between the results markers in the README with the rendered table."""
    text = readme.read_text(encoding="utf-8")
    if START not in text or END not in text:
        raise SystemExit(f"{readme} has no {START} / {END} markers")
    head, rest = text.split(START, 1)
    _, tail = rest.split(END, 1)
    readme.write_text(f"{head}{START}\n{table}\n{END}{tail}", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics", type=Path, default=Path("data/metrics/extract_runs.jsonl"))
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--write-readme", type=Path, default=None, help="README to update between the markers")
    args = ap.parse_args()
    table = render(read_metrics(args.metrics), args.days)
    print(table)
    if args.write_readme:
        splice_readme(args.write_readme, table)


if __name__ == "__main__":
    main()
