"""Unit tests for add_daily_duration function in app/utils.py"""

import polars as pl
from datetime import date
import pytest
from utils import add_daily_duration


def create_test_lazyframe(data: list[dict]) -> pl.LazyFrame:
    """Helper to create a LazyFrame from test data."""
    return pl.LazyFrame(data).select(["JobID", "Start", "End"])


class TestAddDailyDuration:
    """Tests for the add_daily_duration function."""

    def test_same_day_job(self):
        """Test: Job started and ended on the same day."""
        lf = create_test_lazyframe(
            [
                {
                    "JobID": 1,
                    "Start": "2026-02-24T10:00:00",
                    "End": "2026-02-24T14:00:00",
                },
            ]
        )
        result = add_daily_duration(lf, "2026-02-24").collect()

        assert result["daily_duration_hours"][0] == pytest.approx(4.0)

    def test_job_started_previous_day(self):
        """Test: Job started the day before, ended on target day."""
        lf = create_test_lazyframe(
            [
                {
                    "JobID": 2,
                    "Start": "2026-02-23T22:00:00",
                    "End": "2026-02-24T02:00:00",
                },
            ]
        )
        result = add_daily_duration(lf, "2026-02-24").collect()

        # From midnight (00:00) to 02:00 = 2 hours
        assert result["daily_duration_hours"][0] == pytest.approx(2.0)

    def test_job_ends_next_day(self):
        """Test: Job started on target day, ends the next day."""
        lf = create_test_lazyframe(
            [
                {
                    "JobID": 3,
                    "Start": "2026-02-24T20:00:00",
                    "End": "2026-02-25T04:00:00",
                },
            ]
        )
        result = add_daily_duration(lf, "2026-02-24").collect()

        # From 20:00 to midnight (24:00) = 4 hours
        assert result["daily_duration_hours"][0] == pytest.approx(4.0)

    def test_spanning_job(self):
        """Test: Job spans multiple days including target day."""
        lf = create_test_lazyframe(
            [
                {
                    "JobID": 4,
                    "Start": "2026-02-23T12:00:00",
                    "End": "2026-02-25T12:00:00",
                },
            ]
        )
        result = add_daily_duration(lf, "2026-02-24").collect()

        # Full day = 24 hours
        assert result["daily_duration_hours"][0] == pytest.approx(24.0)

    def test_job_not_on_target_day(self):
        """Test: Job ran completely outside the target day."""
        lf = create_test_lazyframe(
            [
                {
                    "JobID": 5,
                    "Start": "2026-02-22T10:00:00",
                    "End": "2026-02-22T14:00:00",
                },
            ]
        )
        result = add_daily_duration(lf, "2026-02-24").collect()

        assert result["daily_duration_hours"][0] == pytest.approx(0.0)

    def test_job_runs_future_day(self):
        """Test: Job runs entirely in the future relative to target day."""
        lf = create_test_lazyframe(
            [
                {
                    "JobID": 6,
                    "Start": "2026-02-25T10:00:00",
                    "End": "2026-02-25T14:00:00",
                },
            ]
        )
        result = add_daily_duration(lf, "2026-02-24").collect()

        assert result["daily_duration_hours"][0] == pytest.approx(0.0)

    def test_midnight_to_midnight(self):
        """Test: Job runs from midnight to midnight (edge case)."""
        lf = create_test_lazyframe(
            [
                {
                    "JobID": 8,
                    "Start": "2026-02-24T00:00:00",
                    "End": "2026-02-25T00:00:00",
                },
            ]
        )
        result = add_daily_duration(lf, "2026-02-24").collect()

        # This should be 24 hours since end is exclusive (at midnight next day)
        assert result["daily_duration_hours"][0] == pytest.approx(24.0)

    def test_multiple_jobs(self):
        """Test: Multiple jobs with different scenarios."""
        lf = create_test_lazyframe(
            [
                {
                    "JobID": 1,
                    "Start": "2026-02-24T10:00:00",
                    "End": "2026-02-24T14:00:00",
                },  # 4h
                {
                    "JobID": 2,
                    "Start": "2026-02-23T22:00:00",
                    "End": "2026-02-24T02:00:00",
                },  # 2h
                {
                    "JobID": 3,
                    "Start": "2026-02-24T20:00:00",
                    "End": "2026-02-25T04:00:00",
                },  # 4h
                {
                    "JobID": 4,
                    "Start": "2026-02-23T12:00:00",
                    "End": "2026-02-25T12:00:00",
                },  # 24h
                {
                    "JobID": 5,
                    "Start": "2026-02-22T10:00:00",
                    "End": "2026-02-22T14:00:00",
                },  # 0h
            ]
        )
        result = add_daily_duration(lf, "2026-02-24").collect()

        expected = [4.0, 2.0, 4.0, 24.0, 0.0]
        for i, exp in enumerate(expected):
            assert result["daily_duration_hours"][i] == pytest.approx(
                exp
            ), f"Job {i+1} failed"

    def test_with_date_object(self):
        """Test: Using date object instead of string."""
        lf = create_test_lazyframe(
            [
                {
                    "JobID": 1,
                    "Start": "2026-02-24T10:00:00",
                    "End": "2026-02-24T14:00:00",
                },
            ]
        )
        result = add_daily_duration(lf, date(2026, 2, 24)).collect()

        assert result["daily_duration_hours"][0] == pytest.approx(4.0)

    def test_half_hour_job(self):
        """Test: Job runs for exactly 30 minutes."""
        lf = create_test_lazyframe(
            [
                {
                    "JobID": 1,
                    "Start": "2026-02-24T10:00:00",
                    "End": "2026-02-24T10:30:00",
                },
            ]
        )
        result = add_daily_duration(lf, "2026-02-24").collect()

        assert result["daily_duration_hours"][0] == pytest.approx(0.5)
