import pandas as pd
from pandas.tseries.frequencies import to_offset

from tshistory.util import diff


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


def history_profiling(
        tsa,
        name,
        from_value_date=None,
        to_value_date=None,
        from_insertion_date=None,
        to_insertion_date=None,
):
    hist = tsa.history(
        name,
        from_value_date=from_value_date,
        to_value_date=to_value_date,
        from_insertion_date=from_insertion_date,
        to_insertion_date=to_insertion_date,
    )
    results = {}
    previous_state = pd.Series()
    for idate, state in hist.items():
        differential = diff(previous_state, state)
        nb_values = len(differential)
        min = differential.min()
        max = differential.max()
        standard_deviation = previous_state.std()
        mean = previous_state.mean()
        deviation_max = abs((max - mean) / standard_deviation)
        deviation_min = abs((min - mean) / standard_deviation)
        previous_state = state.copy()
        if differential.index.tz is None:
            differential.index = differential.tz_localize('UTC').index
        delta_from = differential.index[0] - idate
        delta_to = differential.index[-1] - idate
        results[idate] = {
            'nb_values' : nb_values,
            'delta_from': delta_from,
            'delta_to': delta_to,
            'min': min,
            'max': max,
            'deviation_min': deviation_min,
            'deviation_max': deviation_max,
        }

    return results


def find_dependents(tsa, primary, direct=True):
    catalog = tsa.catalog()
    local_cat = catalog[list(catalog.keys())[0]]
    formulas_cat = [name for name, typ in local_cat if typ=='formula']
    direct_formulas = [
        name
        for name in formulas_cat
        if primary in tsa.formula_components(name)[name]
    ]
    if direct:
        return direct_formulas

    assert tsa.engine, 'find_dependents function need postgres connection'
    result = set(direct_formulas)
    for formula in direct_formulas:
        result = result.union(
            set(tsa.tsh.dependents(tsa.engine, formula, direct=False))
        )
    return sorted(list(result))
