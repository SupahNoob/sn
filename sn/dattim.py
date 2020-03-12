from typing import Union
import calendar
import datetime

from pandas.tseries.holiday import (
    get_calendar, AbstractHolidayCalendar, Holiday, nearest_workday,
    USMartinLutherKingJr, USPresidentsDay, USMemorialDay, USLaborDay,
    USColumbusDay, USThanksgivingDay
)


def nearest_future(weekday: Union[str, int]):
    """
    Find the nearest future weekday.
    
    This function is to be used as a factory for pandas's Holiday().observance.

    Examples
    --------
    >>> from pandas.tseries.holiday import Holiday
    >>>
    >>> next_sunday = nearest_future('sunday')
    >>> rule = Holiday('Super Bowl', month=2, day=1, observance=next_sunday)
    >>> rule.dates('2020-01-15', '2020-02-15')
    DatetimeIndex(['2020-02-02'], dtype='datetime64[ns]', freq=None)

    Parameters
    ----------
    weekday : [str, int]
        name or number of weekday to lean towards
    """
    if isinstance(weekday, str):
        weekday = list(map(str.lower, calendar.day_name)).index(weekday)

    def _wrapper(dt: datetime.datetime) -> datetime.datetime:
        days = ((weekday - dt.weekday()) + 7) % 7
        return dt + datetime.timedelta(days)
    
    return _wrapper


class USBusinessHolidayCalendar(AbstractHolidayCalendar):
    """
    A US Holiday calendar with adjustments for Corporate-Honored Holidays.
    """
    rules = [
        Holiday('New Years Day', month=1, day=1),
        Holiday('New Years Day Observed', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        USMemorialDay,
        Holiday('July 4th', month=7, day=4),
        Holiday('July 4th Observed', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USColumbusDay,
        Holiday('Veterans Day', month=11, day=11),
        Holiday('Veterans Day Observed', month=11, day=11, observance=nearest_workday),
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25),
        Holiday('Christmas Observed', month=12, day=25, observance=nearest_workday),
    ]


class USHolidayCalendar(AbstractHolidayCalendar):
    """
    A US Holiday calendar to denote special days that don't affect working days.
    """
    rules = [
#         Holiday('Superbowl Sunday', month=2, day=1, observance=nearest_future('sunday')),
        Holiday('New Years Eve', month=12, day=31)
    ]
