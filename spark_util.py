from __future__ import print_function
from datetime import date, timedelta
from datetime import datetime as dt


def head_df(df, n = 10):
    return df.limit(n).toPandas().head(n)



def get_agent(phone):
    phone = str(phone)
    viettel = ['8486', '8497', '8498', '8432', '8434', '8435', '8496', '8433', '8439', '8438', '8436', '8437']
    viettel_list = [[a,"viettel"] for a in viettel]
    mobi = ['8479', '8493', '8477', '8490', '8478', '8476', '8489', '8470']
    mobi_list = [[a,"mobi"] for a in mobi]
    vina = ['8481', '8494', '8491', '8488', '8483', '8484', '8482', '8485']
    vina_list = [[a,"vina"] for a in vina]
    all_list = viettel_list + mobi_list + vina_list
    prefix_dict = {key:value for (key, value) in all_list}
    prefix = phone[:4]
    return prefix_dict.get(prefix, "other")

