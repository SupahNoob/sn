import random
import string

from ward import test, fixture, each, skip
import sqlalchemy as sa
import pandas as pd
import numpy as np

from work_pwd.python.sn import to_sqla


@fixture(scope='module')
def dummy_frame():
    dates  = pd.date_range('2020-01-01', '2020-12-31')
    dattim = dates + pd.DateOffset(hours=1)
    floats = np.arange(0.0, len(dates))
    ints   = range(0, len(dates))
    texts  = [random.choice(string.ascii_letters) for c in range(len(dates))]

    df = pd.DataFrame()\
           .assign(
               dates_not_null=dates,
               dates_and_null=[np.nan, *dates[1:]],
               datetimes_not_null=dattim,
               datetimes_and_null=[np.nan, *dattim[1:]],
               dts_tz_not_null=dattim.tz_localize('UTC'),
               dts_tz_and_null=[np.nan, *dattim.tz_localize('UTC')[1:]],
               floats_not_null=floats,
               floats_and_null=[np.nan, *floats[1:]],
               ints_not_null=ints,
               ints_and_null=[np.nan, *ints[1:]],
               text_not_null=texts,
               text_and_null=[np.nan, *texts[1:]]
           )

    return df

@fixture
def dates_not_null(df=dummy_frame):
    return df['dates_not_null']

@fixture
def dates_and_null(df=dummy_frame):
    return df['dates_and_null']

@fixture
def datetimes_not_null(df=dummy_frame):
    return df['datetimes_not_null']

@fixture
def datetimes_and_null(df=dummy_frame):
    return df['datetimes_and_null']

@fixture
def dts_tz_not_null(df=dummy_frame):
    return df['dts_tz_not_null']

@fixture
def dts_tz_and_null(df=dummy_frame):
    return df['dts_tz_and_null']

@fixture
def floats_not_null(df=dummy_frame):
    return df['floats_not_null']

@fixture
def floats_and_null(df=dummy_frame):
    return df['floats_and_null']

@fixture
def ints_not_null(df=dummy_frame):
    return df['ints_not_null']

@fixture
def ints_and_null(df=dummy_frame):
    return df['ints_and_null']

@fixture
def text_not_null(df=dummy_frame):
    return df['text_not_null']

@fixture
def text_and_null(df=dummy_frame):
    return df['text_and_null']



@test('[sn.to_sqla] pandas {s.name} --> {target.__name__}')
def _(
    s=each(
        dates_not_null, dates_and_null,
        datetimes_not_null, datetimes_and_null,
        dts_tz_not_null, dts_tz_and_null,
        floats_not_null, floats_and_null,
        ints_not_null, ints_and_null,
        text_not_null, text_and_null
    ),
    target=each(
        sa.Date, sa.Date,
        sa.DateTime, sa.DateTime,
        sa.TIMESTAMP, sa.TIMESTAMP,
        sa.Float, sa.Float,
        sa.Integer, sa.Integer,
        sa.Text, sa.Text
    )
):
    r = to_sqla(s)
    assert_flag = 0
    
    # NOTE:
    #
    # ward doesn't support multiple asserts so we simply track a flag as to
    # whether or not we agree with test failure

    # INT column with NaN/NULL fixed post pandas 1.0.0
    if int(pd.__version__.split('.')[0]) < 1 and s.name == 'ints_and_null':
        if s.dtype == 'float64':
            assert_flag = 2

    if isinstance(r, target):
        assert_flag = 1

    if r == target:
        assert_flag = 2

    assert assert_flag > 0


@test('[sn.SNDF] .reflect({kwargs}) is represented by {output.__name__}')
def _(
    df=each(dummy_frame, dummy_frame, dummy_frame),
    kwargs=each({}, {'as_sql_stmt': True}, {'pk': ['dates_not_null']}),
    output=each(sa.Table, str, sa.Table)
):
    engine = sa.create_engine('sqlite://')
    model = df.sn.reflect(bind=engine, **kwargs)
    
    if isinstance(model, str):
        # black-listed terms
        ignore = ['CREATE TABLE', 'PRIMARY', 'FOREIGN', 'INDEX', 'CONSTRAINT']
        # ignore the trailing ")" line at either end
        stmt = model.strip().split('\n')[:-1]
        n_columns = len([line for line in stmt if not any(i in line for i in ignore)])
    else:
        n_columns = len(model.columns)
    
    assert type(model) is output
    assert len(df.columns) == n_columns
