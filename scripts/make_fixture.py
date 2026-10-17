"""Build the committed test fixture: two days of observations for Florida CoCoRaHS stations.

Run once (needs network):  uv run python scripts/make_fixture.py
The output is small (a few thousand rows) and is committed under tests/fixtures/.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ghcn_elt.extract import connect, observations_query
from ghcn_elt.sources import Window, observation_globs

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "observations_sample.parquet"


def main() -> None:
    con = connect()
    window = Window(date(2026, 10, 1), date(2026, 10, 2))
    query = observations_query(observation_globs("s3://noaa-ghcn-pds", window.years), window, "US1FL")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO '{OUT.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{OUT.as_posix()}')").fetchone()
    print(f"wrote {OUT} rows={n[0] if n else 0} bytes={OUT.stat().st_size}")


if __name__ == "__main__":
    main()
