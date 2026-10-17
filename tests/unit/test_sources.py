from datetime import date

import pytest

from ghcn_elt.sources import (
    Window,
    lookback_window,
    observation_globs,
    parse_station_line,
    parse_stations,
    stations_url,
)

# Two real lines from ghcnd-stations.txt (fixed width, positions from NOAA's readme).
TAMPA_INTL = "USW00012842  27.9633  -82.5400    1.8 FL TAMPA INTL AP                  GSN     72211"
COCORAHS_FL = "US1FLHL0004  27.5958  -81.4898   53.0 FL AVON PARK 0.9 ENE                           "
LAX_LINE = "USW00023174  33.9381 -118.3867   29.6 CA LOS ANGELES INTL AP                    72295"
# Same line with the HCN/CRN flag columns (77-79) filled, to pin the column positions.
HCN_LINE = LAX_LINE[:76] + "HCN" + LAX_LINE[79:]


def test_lookback_window_covers_previous_days_inclusive() -> None:
    w = lookback_window(date(2026, 10, 17), lookback_days=14, start_date=date(2024, 1, 1))
    assert w == Window(start=date(2026, 10, 3), end=date(2026, 10, 17))
    assert w.days == 15


def test_lookback_window_is_clamped_to_start_date() -> None:
    w = lookback_window(date(2024, 1, 5), lookback_days=14, start_date=date(2024, 1, 1))
    assert w.start == date(2024, 1, 1)
    assert w.days == 5


def test_window_crossing_new_year_needs_both_partitions() -> None:
    w = lookback_window(date(2026, 1, 3), lookback_days=14, start_date=date(2024, 1, 1))
    assert w.years == [2025, 2026]
    globs = observation_globs("s3://noaa-ghcn-pds", w.years)
    assert globs == [
        "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2025/*/*.parquet",
        "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2026/*/*.parquet",
    ]


def test_run_before_start_date_is_rejected() -> None:
    with pytest.raises(ValueError, match="before the pipeline start date"):
        lookback_window(date(2023, 12, 31), 14, date(2024, 1, 1))


def test_window_rejects_inverted_range() -> None:
    with pytest.raises(ValueError):
        Window(start=date(2026, 1, 2), end=date(2026, 1, 1))


def test_parse_station_line_airport() -> None:
    s = parse_station_line(TAMPA_INTL)
    assert s.station_id == "USW00012842"
    assert s.latitude == pytest.approx(27.9633)
    assert s.longitude == pytest.approx(-82.5400)
    assert s.elevation_m == pytest.approx(1.8)
    assert s.state == "FL"
    assert s.name == "TAMPA INTL AP"
    assert s.gsn_flag is True
    assert s.hcn_crn_flag is None
    assert s.wmo_id == "72211"


def test_parse_station_line_flags_and_missing_fields() -> None:
    lax = parse_station_line(LAX_LINE)
    assert lax.gsn_flag is False
    assert lax.hcn_crn_flag is None
    assert lax.wmo_id == "72295"
    hcn = parse_station_line(HCN_LINE)
    assert hcn.hcn_crn_flag == "HCN"
    assert hcn.wmo_id == "72295"
    c = parse_station_line(COCORAHS_FL)
    assert c.name == "AVON PARK 0.9 ENE"
    assert c.elevation_m == pytest.approx(53.0)
    assert c.wmo_id is None
    assert c.hcn_crn_flag is None


def test_parse_station_line_missing_elevation_becomes_null() -> None:
    line = "USC00000001  10.0000  -80.0000 -999.9 FL NOWHERE                                  "
    assert parse_station_line(line).elevation_m is None


def test_parse_station_line_too_short_is_rejected() -> None:
    with pytest.raises(ValueError, match="too short"):
        parse_station_line("USW00012842  27.9619")


def test_parse_stations_filters_by_prefix_and_skips_blank_lines() -> None:
    text = "\n".join(
        [
            TAMPA_INTL,
            "",
            "CA001010066  48.8667 -123.2833    4.0    ACTIVE PASS                            71030",
            LAX_LINE,
        ]
    )
    us = parse_stations(text, prefix="US")
    assert [s.station_id for s in us] == ["USW00012842", "USW00023174"]
    assert len(parse_stations(text)) == 3


def test_stations_url() -> None:
    assert stations_url("https://noaa-ghcn-pds.s3.amazonaws.com/") == (
        "https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt"
    )
