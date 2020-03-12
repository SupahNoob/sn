from typing import Optional

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, ForeignKey,
    Boolean, SmallInteger, Integer, Date, String
)
import sqlalchemy as sa
import pandas as pd


Base = declarative_base()


class BusinessCalendar(Base):
    """
    Model for a denormalized Date Dimension.

    NOTE:
        Since a calendar DATE cannot repeat, it is a perfect Natural Key. Some
        developers prefer to use a surrogate key here, but I do not find it to
        be necessary. The line is included below in order to create a surrogate,
        but again, it does not feel conceptually necessary.
    """
    __tablename__ = 'business_calendar'
    # date_id = Column(Integer, primary_key=True)
    calendar_date = Column(Date, primary_key=True)
    day_of_week = Column(SmallInteger, comment='zero-indexed number associated with the weekday - Monday = 0, Sunday = 6')
    day_of_month = Column(SmallInteger)
    day_of_year = Column(SmallInteger)
    weekday = Column(String)
    weekday_in_month = Column(SmallInteger, comment='the occurrence of this weekday within the month - 1st Sunday = 1, 3rd Monday = 3')
    weekday_number = Column(SmallInteger)
    week_begin = Column(Date)
    week_end = Column(Date)
    week_of_month = Column(SmallInteger)
    week_of_year = Column(SmallInteger)
    month_begin = Column(Date)
    month_end = Column(Date)
    month_of_quarter = Column(SmallInteger)
    month_of_year = Column(SmallInteger)
    month_name = Column(String)
    quarter_begin = Column(Date)
    quarter_end = Column(Date)
    quarter_of_year = Column(SmallInteger)
    is_business_day = Column(Boolean)
    is_weekday = Column(Boolean)
    is_weekend = Column(Boolean)
    is_us_holiday = Column(Boolean)

    @staticmethod
    def populate(
        start_date: str,
        end_date: str,
        engine: sa.engine.Engine=None,
    ) -> Optional[pd.DataFrame]:
        """
        Fills the database with data.
        
        Parameters
        ----------
        start_date : str
            beginning DATE for the table in the format YYYY-MM-DD

        end_date : str
            ending DATE for the table in the format YYYY-MM-DD
        
        engine : sqlalchemy.engine.Engine
            engine instance for INSERT of data into a database

        Returns
        -------
        df : pandas.DataFrame
        """
        df = pd.date_range(start_date, end_date)\
               .to_frame(index=False, name='calendar_date')\
               .assign(
                   day_of_month=BusinessCalendar._from_calendar_date('day', attr=True),
                   day_of_year=BusinessCalendar._from_calendar_date('dayofyear', attr=True),
                   weekday=BusinessCalendar._from_calendar_date('day_name'),
                   weekday_number=BusinessCalendar._from_calendar_date('weekday', attr=True),
                   weekday_in_month=lambda df: ((df.day_of_month - 1) // 7) + 1,
                   week_begin=lambda df: df.calendar_date - (df.weekday_number * np.timedelta64(1, 'D')),
                   week_end=lambda df: df.week_begin + np.timedelta64(6, 'D'),
                   week_of_month=lambda df: round(df.day_of_month // 7) + 1,
                   week_of_year=lambda df: round(df.day_of_year // 7) + 1,
                   month_begin=lambda df: df.calendar_date - MonthBegin(1),
                   month_end=lambda df: df.month_begin + MonthEnd(1),
                   month_of_quarter=lambda df: df.calendar_date.dt.month // df.calendar_date.dt.quarter,
                   month_of_year=BusinessCalendar._from_calendar_date('month', attr=True),
                   month_name=BusinessCalendar._from_calendar_date('strftime', date_format='%B'),
                   quarter_begin=lambda df: df.calendar_date + QuarterBegin(0, startingMonth=1),
                   quarter_end=lambda df: df.quarter_begin + MonthEnd(3),
                   quarter_of_year=BusinessCalendar._from_calendar_date('quarter', attr=True),
                   is_weekday=lambda df: df.weekday_number < 5,
                   is_weekend=lambda df: ~df.is_weekday
               )\
               .pipe(BusinessCalendar._set_holidays)\
               .assign(is_business_day=lambda df: ~df.is_us_holiday & df.is_weekday)\
               .sort_values('calendar_date')
        
        try:
            # TODO: currently this assumes the table has been created already
            with engine.connect() as conn:
                stmt = BusinessCalendar.__table__.insert()
                data = df.to_dict(orient='records')
                conn.execute(stmt, data)

        except AttributeError:
            pass
        else:
            return
        return df

    @staticmethod
    def _from_calendar_date(name, *args, attr=False, **kwargs):
        def _wrapper(df):
            r = getattr(df.calendar_date.dt, name)

            if attr:
                return r
            return r(*args, **kwargs)
        return _wrapper

    @staticmethod
    def _set_holidays(df, calendar_name='USBusinessHolidayCalendar'):
        min_ = df.calendar_date.min()
        max_ = df.calendar_date.max()
        cf = get_calendar(calendar_name).holidays(min_, max_, return_name=True)\
                                        .to_frame()\
                                        .reset_index()\
                                        .rename(columns={'index': 'calendar_date', 0: 'day_name'})\
                                        .drop_duplicates('calendar_date')

        df = df.merge(cf, how='left', on='calendar_date')\
               .assign(is_us_holiday=lambda df: df.day_name.notna())

        return df
