import pandas as pd
from pandas.tseries.frequencies import to_offset


def infer_freq(ts):
    return _infer_freq(ts.index)


def _infer_freq(timestamps):
    assert len(timestamps) > 1, 'infer_freq needs at least 2 timestamp'
    index = pd.Series(timestamps)
    deltas = (index - index.shift(1)).dropna()
    freq = deltas.median()
    conform_intervals = sum(deltas == freq)
    return freq, conform_intervals / len(deltas)


def infer_values_frequency(
        tsa,
        name,
        revision_date=None,
        from_value_date=None,
        to_value_date=None,
):
    ts = tsa.get(name, revision_date, from_value_date, to_value_date)
    if ts is None or len(ts) < 2:
        return None
    return infer_freq(ts)


def infer_insertions_frequency(
        tsa,
        name,
        from_insertion_date=None,
        to_insertion_date=None,
        from_value_date=None,
        to_value_date=None,
):
    idates = tsa.insertion_dates(
        name,
        from_insertion_date,
        to_insertion_date,
        from_value_date,
        to_value_date,
    )
    return _infer_freq(idates)[0]


def find_missing_value_dates(
        tsa,
        name,
        revision_date=None,
        from_value_date=None,
        to_value_date=None,
):
    ts = tsa.get(
        name,
        revision_date=revision_date,
        from_value_date=from_value_date,
        to_value_date=to_value_date,
    )
    if ts is None or len(ts) < 2:
        return None
    inferred_freq = infer_freq(ts)[0]
    first_value = ts.index[0]
    last_value = ts.index[-1]
    freq_offset = to_offset(inferred_freq)
    regular_dates = pd.date_range(
        start=first_value,
        end=last_value,
        freq=freq_offset
    )
    missing_dates = regular_dates[~regular_dates.isin(ts.index)]
    return missing_dates.to_list()
