from __future__ import print_function

from datetime import datetime as dt
from datetime import timedelta



PY_NORM_FDATE = "%Y-%m-%d"
SPARK_NORM_FDATE = "dd-MM-yyyy"

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

