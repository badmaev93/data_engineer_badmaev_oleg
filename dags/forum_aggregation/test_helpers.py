import pytest
from datetime import datetime
from dags.forum_aggregation.helpers import _resolve_dates

def test_resolve_dates_valid_params():
    context = {"params": {"date_from": "2026-10-01", "date_to": "2026-10-31"}}
    date_from, date_to = _resolve_dates(context)
    assert date_from == "2026-10-01"
    assert date_to == "2026-10-31"

def test_resolve_dates_invalid_params():
    context = {"params": {"date_from": "2026-10-01; DROP TABLE users;", "date_to": "2026-10-31"}}
    with pytest.raises(ValueError):
        _resolve_dates(context)

def test_resolve_dates_ds():
    context = {"ds": "2026-10-15"}
    date_from, date_to = _resolve_dates(context)
    assert date_from == "2026-10-15"
    assert date_to == "2026-10-15"

def test_resolve_dates_no_dates():
    context = {}
    date_from, date_to = _resolve_dates(context)
    assert date_from == datetime.now().strftime("%Y-%m-%d")
    assert date_to == datetime.now().strftime("%Y-%m-%d")
