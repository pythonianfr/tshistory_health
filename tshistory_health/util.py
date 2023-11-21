import pandas as pd


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
