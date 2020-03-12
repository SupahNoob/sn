from typing import Union, Callable
import warnings
import logging
import gc
import io

from pandas.api.extensions import register_dataframe_accessor
from sqlalchemy.types import (
    BigInteger, Integer, Float, Text, Boolean,
    DateTime, Date, Time, TIMESTAMP
)
import sqlalchemy as sa
import pandas as pd
import numpy as np

from .log import LazyStr


_logger = logging.getLogger('sn')
_logger.setLevel('DEBUG')
_logger.addHandler(logging.NullHandler())


def to_sqla(column: pd.Series) -> sa.Column:
    """
    Infer a pandas Series's sqlalchemy column type.

    Further Reading:
        1) see pandas.io.sql#L944 for dtype --> sql type conversion
    """
    # remove evidence of NULLs in column
    # (enforcing NULL constraint should be the responsibility of SQLA)
    column = column[column.notna()]

    # skipna=True is default for .infer_dtype in pd 1.0.0
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=FutureWarning)

        # infer type of column
        col_type = pd._libs.lib.infer_dtype(column)

    if col_type in ['datetime64', 'datetime']:
        try:
            if column.dt.tz is not None:
                return TIMESTAMP(timezone=True)
        except AttributeError:
            if column.tz is not None:
                return TIMESTAMP(timezone=True)

        if (column.dt.hour == 0).all():
            return Date
        return DateTime

    translation = {
        'timedelta64': BigInteger,
        'floating': 'floating',
        'integer': 'integer',
        'boolean': Boolean,
        'date': Date,
        'time': Time,
        'complex': ValueError('complex dtypes not supported')
    }

    try:
        sa_type = translation[col_type]
    except KeyError:
        sa_type = Text
    else:
        if isinstance(sa_type, str):
            if sa_type == 'floating':
                if column.dtype == 'float32':
                    sa_type = Float(precision=23)
                else:
                    sa_type = Float(precision=53)

            if sa_type == 'integer':
                if column.dtype == 'int32':
                    sa_type = Integer
                else:
                    sa_type = BigInteger

        elif isinstance(sa_type, ValueError):
            raise sa_type

    return sa_type


@register_dataframe_accessor('sn')
class SNDF:
    """
    An extension to the pandas DataFrame.

    All methods may be accessed via the attribute "sn". Simply all that is
    necessary to enable this functionality, is to import SNDF into your runtime
    environment.

    All of these dataframe methods exist to make common operations that this
    developer has found, easier, simpler, or more informative.
    """
    def __init__(self, df):
        self._pre_validate(df)
        self._df = df

    @staticmethod
    def _pre_validate(df):
        """
        Lorem ipsum...

        This function runs one time on the following conditions:
          - the instantiation of a NEW dataframe
              AND once the attribute "sn" is accessed

        It's important to remember that many pandas functions
        return a new dataframe object (as opposed to operation
        "in_place") and the next "sn" access would therefore
        trigger this function.

        As of writing this (2020/01/28), the author has yet to
        determine a viable use case for the method, but wanted
        to document its existence in case one might appear.
        """
        pass

    def is_valid_pk(self, column_name: str) -> pd.DataFrame:
        """
        """
        return self._df.shape[0] == len(self._df[column_name].unique())

    def comment(
        self,
        msg: str,
        *,
        log: Callable=_logger,
        level: str='info',
        info: bool=False
    ) -> pd.DataFrame:
        """
        Insert a comment - no transformation happens to df.

        Similar to logging, there are variables which are available for deferred
        evaluation..

            df = the DataFrame being acted upon
            BRANCH = '├─'
            CONT   = '│ '
            FINAL  = '└─'

        Usage
        -----
        df.rename(columns={'Test FN': 'test_fn'})\
          .sn.comment('creation of dataframe')\
          .sn.comment('beginning of ETL:')\
          .sn.comment('{FINAL} adding new column: x')
          .assign(x=lambda df: df.index ** 2)
        """
        BRANCH = '├─'
        CONT   = '│ '
        FINAL  = '└─'

        try:
            log = getattr(log, level)
        except AttributeError:
            pass

        if info:
            def _pipeline():
                """
                Writes df.info to a string-like.
                """
                buffer = io.StringIO()
                self._df.info(buf=buffer)
                return buffer.getvalue()

            lazy = LazyStr(_pipeline)
            msg += '\n\n{lazy}'

        log(msg.format(**locals()))
        return self._df

    def reflect(
        self,
        table_name: str='TMP_dataframe',
        *,
        bind: sa.engine.Engine,
        pk: Union[str, list]=None,
        dtypes: dict=None,
        as_sql_stmt: bool=False
    ) -> sa.Table:
        """
        Generate a SQLAlchemy Model based on pandas dtypes.

        Usage
        -----
        engine = sqlalchemy.create_engine('sqlite://')
        df = df.copy()
        model = df.sn.reflect('TMP_data', bind=engine)

        stmt = model.insert().values(df.to_dict(orient='records'))
        engine.execute(stmt)

        Arguments
        ---------
        table_name : str = [default: 'TMP_dataframe']
            TODO

        bind : sqlalchemy.engine.Engine
            TODO

        pk : list = [default: []]
            TODO

        dtype : dict = [default: {}]
            TODO

        as_sql_stmt : bool = [default: False]
            TODO

        Returns
        -------
        model : sqlalchemy.Table
        """
        if dtypes is None:
            dtypes = {}
        if pk is None:
            pk = []
        if isinstance(pk, str):
            pk = [pk]

        c_name_and_types = []

        for i, name in enumerate(self._df.columns):
            type = dtypes.get(name, to_sqla(self._df.iloc[:, i]))
            is_pk = name.lower() in map(lambda s: s.lower(), pk)
            c_name_and_types.append((name, type, is_pk))

        columns = [
            sa.Column(name, type, primary_key=is_pk)
            for name, type, is_pk in c_name_and_types
        ]

        tbl = sa.Table(table_name, sa.MetaData(), *columns)

        if as_sql_stmt:
            stmt = sa.schema.CreateTable(tbl)\
                            .compile(dialect=bind.dialect)

            return str(stmt)
        return tbl

    def index_statistics(self) -> pd.DataFrame:
        """
        Generate statistics to determine index candidacy.

        A good candidate for an Index will meet a few criteria:

            - Frequent business-use in WHERE, GROUP BY, ORDER BY clauses
            - High selectivity / Low cardinality
            - Not a high percentage of NULL values

        Returns
        -------
        sel : pd.Series
        """
        data = []

        for column_name in self._df.columns:
            s = self._df[column_name]
            data.append({
                'null_pct': (1 - sum(s.notna()) / len(s)) * 100,
                'cardinality': len(s.unique()),
                'selectivity': len(s.unique()) / len(s) * 100,
                'fully_unique': len(s.unique()) == len(s)
            })

        return pd.DataFrame(data, index=self._df.columns)


def reduce_mem_usage(df):
    """
    Perform a series of operations to reduce memory usage.

    Downscale Numerics
    Categorize Strings

    TODO convert DateTime to Date if all 0:00 time
    TODO attach to SNDF
    """
    start_mem = df.memory_usage().sum() / 1024**2
    _logger.info('Memory usage of dataframe is {:.2f} MB'.format(start_mem))

    for col in df.columns:
        col_type = df[col].dtype
        gc.collect()
        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        else:
            # TODO take into account cardinality.
            df[col] = df[col].astype('category')

    end_mem = df.memory_usage().sum() / 1024**2
    _logger.info('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
    _logger.info('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))

    return df
