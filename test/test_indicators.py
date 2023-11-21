from datetime import datetime, timedelta

import pandas as pd
import pytest

from tshistory_health.util import (
    infer_freq,
    infer_values_frequency,
    infer_insertions_frequency,
    find_missing_value_dates,
)

def test_infer_freq():
    s1 = pd.Series(
        [1., 2., 3., 4.],
        index=pd.date_range(datetime(2020, 1, 1), freq='H', periods=4)
    )
    d, q = infer_freq(s1)
    assert d == pd.Timedelta(hours=1)
    assert q == 1

    s2 = pd.Series(
        [1., 2., 3., None, 5.],
        index=pd.date_range(datetime(2020, 1, 1), freq='H', periods=5)
    )
    d, q = infer_freq(s2.dropna())
    assert d == pd.Timedelta(hours=1)
    assert q == 0.6666666666666666

    s3 = pd.Series(
        [1.],
        index=pd.date_range(datetime(2020, 1, 1), freq='H', periods=1)
    )
    with pytest.raises(AssertionError):
        infer_freq(s3)


def test_values_freq(tsa):
    s1 = pd.Series(
        [1., 2., 3., 4.],
        index=pd.date_range(datetime(2020, 1, 1), freq='H', periods=4)
    )
    tsa.update('my-hourly-series', s1, 'test-health')

    d, q = infer_values_frequency(tsa, 'my-hourly-series')
    assert d == pd.Timedelta('0 days 01:00:00')
    assert q == 1


def test_insertions_freq(tsa):
    ts =  pd.Series(
        [1., 2., 3., 4.],
        index=pd.date_range(datetime(2020, 1, 1), freq='D', periods=4)
    )
    idate = pd.Timestamp('2023-01-01')
    for idx in range(10):
        ts = ts + idx
        tsa.update(
            'multiple-insertions',
            ts,
            'test',
            insertion_date=idate + timedelta(days=idx),
        )

    assert infer_insertions_frequency(
        tsa, 'multiple-insertions'
    ) == pd.Timedelta('1 days 00:00:00')


def test_find_missing_values(tsa):
    ts = pd.Series(
        [1., 2., 3., 4.],
        index=pd.date_range(datetime(2020, 1, 1), freq='D', periods=4)
    )
    tsa.update('series-to-complete', ts, 'test-health')

    missing_dates = find_missing_value_dates(tsa, 'series-to-complete')

    assert missing_dates == []

    # we add two chunks to this series
    # we left one missing day (2020-01-05)
    ts = pd.Series(
        [1., 2., 3., 4.],
        index=pd.date_range(datetime(2020, 1, 6), freq='D', periods=4)
    )
    tsa.update('series-to-complete', ts, 'test-health')

    # we left two missing days (2020-01-10 and 2020-01-11)
    ts = pd.Series(
        [1., 2., 3., 4.],
        index=pd.date_range(datetime(2020, 1, 12), freq='D', periods=4)
    )
    tsa.update('series-to-complete', ts, 'test-health')

    missing_dates = find_missing_value_dates(tsa, 'series-to-complete')

    assert missing_dates == [
        pd.Timestamp('2020-01-05 00:00:00'),
        pd.Timestamp('2020-01-10 00:00:00'),
        pd.Timestamp('2020-01-11 00:00:00')
    ]


def test_missing_values_with_non_regular_series(tsa):
    ts = pd.Series(
        range(12),
        index = pd.date_range(datetime(2023, 1, 1), freq='M', periods=12)
    )
    tsa.update('monthly-series', ts, 'test-health')

    # the monthly series is by construct irregular:
    # our heuristic can only returns stupid results
    missing_dates = find_missing_value_dates(tsa, 'monthly-series')
    assert missing_dates == [
        pd.Timestamp('2023-03-03 00:00:00', freq='31D'),
        pd.Timestamp('2023-04-03 00:00:00', freq='31D'),
        pd.Timestamp('2023-05-04 00:00:00', freq='31D'),
        pd.Timestamp('2023-06-04 00:00:00', freq='31D'),
        pd.Timestamp('2023-07-05 00:00:00', freq='31D'),
        pd.Timestamp('2023-08-05 00:00:00', freq='31D'),
        pd.Timestamp('2023-09-05 00:00:00', freq='31D'),
        pd.Timestamp('2023-10-06 00:00:00', freq='31D'),
        pd.Timestamp('2023-11-06 00:00:00', freq='31D'),
        pd.Timestamp('2023-12-07 00:00:00', freq='31D'),
    ]
