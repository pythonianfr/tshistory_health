from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pytest_sa_pg import db
from sqlalchemy import create_engine

from dbcache import schema as schema_cache
from tshistory.api import timeseries
from tshistory.schema import tsschema


DATADIR = Path(__file__).parent / 'test' / 'data'


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port)
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    e = create_engine(uri)
    schem = tsschema('tsh')
    schem.create(e)
    yield e


@pytest.fixture(scope='session')
def tsa(engine):
    return timeseries(str(engine.url), sources={})