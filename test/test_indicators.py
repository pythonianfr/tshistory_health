from datetime import datetime

import pandas as pd
import pytest

from tshistory_health.util import (
    infer_freq,
    infer_values_frequency,
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