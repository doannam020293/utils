from __future__ import print_function

from datetime import datetime as dt
from datetime import timedelta, date
from pyspark.sql import functions as F
from dateutil.relativedelta import relativedelta




PY_NORM_FDATE = "%Y-%m-%d"
SPARK_NORM_FDATE = "yyyy-MM-dd"


def get_end_date_month(date):
    """

    :param date: instance of date
    :return:
    """
    return date + relativedelta(day=31)


def get_sunday_process(curdate):
    """
    if curdate is T2, T3 => return CN tuần trước nữa
    if curdate is T4-> CN => return CN tuần này

    Args:
        cur_date (string/date): input date

    Returns:
        date: the date of Sunday in the week of input date
    """
    curdate = dt.strptime(curdate, "%Y-%m-%d") if type(curdate) is str else curdate
    wod = curdate.weekday()
    if (wod in (0, 1)):
        diff = wod + 8
    else:
        diff = wod + 1
    return curdate - timedelta(diff)

def get_this_sunday(cur_date):
    """
    Return the date of Sunday in the week of input date

    Args:
        cur_date (string/date): input date

    Returns:
        date: the date of Sunday in the week of input date
    """
    cur_date = dt.strptime(cur_date, "%Y-%m-%d") if type(cur_date) is str else cur_date
    return cur_date - timedelta(cur_date.weekday() - 6)
from pyspark.sql import functions as F
def udf_get_sunday(date):
    return F.date_sub(F.next_day(date, "sunday"), 7)

def is_sunday(curdate):
    (year,week,dow) = curdate.isocalendar()
    return dow == 7


def date_range(start_date, end_date, duration=1, return_type="date"):
    """
    return list of date range, not include end_date
    :param start_date: date(2010,1,1)
    :param end_date: date(2010,5,1)
    :param duration: 1
    :param return_type: "str", or "date"

    :return: list
    """
    if (not isinstance(start_date, date)) or (not isinstance(end_date, date)):
        print("need start_date, and end_date datetime.date instance")
        return
    day_count = (end_date - start_date).days
    result = [start_date + timedelta(n) for n in range(0, day_count, duration)]
    if return_type == "str":
        result = [dt.strftime(a, PY_NORM_FDATE) for a in result]
    return result

