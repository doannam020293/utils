from __future__ import print_function
from datetime import date, timedelta
from datetime import datetime as dt
import subprocess
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
import os

from etl.common.hdfs import check_dir_exist,get_size,rename_file, delete_file

spark = SparkSession.builder.getOrCreate()

def _back_fill_data(in_dir, out_dir, start_date, end_date, min_size, backfill_type="last"):
    day_count = (end_date - start_date).days
    size_all = get_size(in_dir)
    last_date_str = dt.strftime(end_date, "%Y-%m-%d")

    for curdate in (end_date - timedelta(n) for n in range(day_count)):
        curdate_str = dt.strftime(curdate, "%Y-%m-%d")
        in_file = os.path.join(in_dir, "date={}".format(curdate_str))
        #     print( "curdate_str", curdate_str)
        out_file = os.path.join(out_dir, "date={}".format(curdate_str))
        size = size_all.get(curdate_str, 0)
        if size < min_size:
            print( "back fill date {0} by date {1}".format(curdate_str, last_date_str))
            curdate_str = last_date_str
            in_file = os.path.join(in_dir, "date={}".format(curdate_str))
        print(  "in_file", in_file)
        print(  "out_file", out_file)
        df = spark.read.parquet(in_file)
        df.write.mode("overwrite").parquet(out_file)
        last_date_str = curdate_str


def back_fill_data(in_dir, out_dir, start_date, end_date, min_count, delta_date=7, write_all=True):
    """
    :param in_dir:
    :param out_dir:
    :param start_date: like date(2018,1,1)
    :param end_date:
    :param min_count:
    :param delta_date:
    :param write_all:write all backfill to other folder.
    :return:
    """

    def get_date_backkfill(df_group, curdate, delta_date, min_count):
        df_backfill = df_group[df_group['count1'] >= min_count]
        dict_count = dict(df_backfill.values)
        cur_count = dict_count.get(curdate, 0)
        for i in range(0, 100, delta_date):
            back_date = curdate + timedelta(i)
            back_count = dict_count.get(back_date, 0)
            if back_count >= min_count:
                return back_date
        return df_backfill[df_backfill['date'] >= curdate]['date'].min()

    df = spark.read.parquet(in_dir)
    df_group = df.groupBy("date").agg(F.count("*").alias("count1")) \
        .toPandas()

    day_count = (end_date - start_date).days
    for curdate in (start_date + timedelta(n) for n in range(day_count)):
        curdate_str = dt.strftime(curdate, "%Y-%m-%d")
        out_file = os.path.join(out_dir, "date={}".format(curdate_str))
        in_date = get_date_backkfill(df_group, curdate, delta_date, min_count)
        in_date_str = dt.strftime(in_date, "%Y-%m-%d")
        print("back fill date {0} by date {1}".format(curdate_str, in_date_str))
        in_file = os.path.join(in_dir, "date={}".format(in_date_str))
        df = spark.read.parquet(in_file)
        if write_all or (in_date != curdate):
            df.write.mode("overwrite").parquet(out_file)


def dedup_file(in_file, order_columns=["phone_number", ]):
    backup_file = in_file + ".bak"
    in_file_exist_status = check_dir_exist(in_file)
    backup_file_exist_status = check_dir_exist(backup_file)
    if not in_file_exist_status and not backup_file_exist_status:
        print(in_file, backup_file, "not exist")
    if not backup_file_exist_status:
        if not rename_file(in_file, backup_file):
            print( "rename_file ERROR", in_file, backup_file)
            exit(1)
        else:
            print( backup_file, "already existed")

    if len(order_columns) > 0:
        print( "order!!!")
        df = spark.read.parquet(backup_file) \
            .orderBy(*order_columns) \
            .dropDuplicates()
    else:
        df = spark.read.parquet(backup_file) \
            .dropDuplicates()

    df.write.mode("overwrite").parquet(in_file)
    print(delete_file(backup_file))


def dedup_folder(in_dir, start_date=None, end_date=None, order_columns=["phone_number", ]):
    size_all = get_size(in_dir)
    if start_date == None:
        date_list = sorted([dt.strptime(a, "%Y-%m-%d") for a in size_all.keys()], )
        start_date = date_list[0]
    if end_date == None:
        date_list = sorted([dt.strptime(a, "%Y-%m-%d") for a in size_all.keys()], )
        end_date = date_list[-1] + timedelta(1)

    day_count = (end_date - start_date).days
    for curdate in (start_date + timedelta(n) for n in range(day_count)):
        curdate_str = dt.strftime(curdate, "%Y-%m-%d")
        in_file = "{}/{}".format(in_dir, "date={}".format(curdate_str))
        print( in_file)
        if size_all.get(curdate_str, 0) == 0:
            print( "{} not exist".format(in_file))
            continue
        dedup_file(in_file, order_columns)





