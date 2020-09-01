from __future__ import print_function
from datetime import date, timedelta
from datetime import datetime as dt
import subprocess
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
import os
import sys
import pandas as pd
import seaborn as sns


from cic_util.hdfs import check_dir_exist, get_size, start_date_path, end_date_path
import matplotlib.pyplot as plt
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 20, 8


spark = SparkSession.builder.getOrCreate()


def plot_daily_box_plot(dp, col_name_list=None, date_col_name="date", showfliers=False):
    if col_name_list is None:
        col_name_list = [a for a in dp.columns if a not in ("phone_number", date_col_name, "local_id")]

    dp = dp.sort_values(date_col_name)
    showfliers = []
    for c in col_name_list:
        print( c)
        ax0 = sns.boxplot(x=date_col_name, y=c, data=dp, showfliers=c in showfliers)
        ax0.set_xticklabels(ax0.get_xticklabels(), rotation=45)
        ax0.set_title(c)
        plt.show()


def plot_daily_avg(df, col_name_list=None, date_col_name="date"):
    if col_name_list is None:
        col_name_list = [a for a in df.columns if a not in ("phone_number", "date", "local_id")]
    for col_name in col_name_list:
        print( col_name)
        dp = df.groupBy("date").agg(F.avg(col_name).alias("avg_ft_" + col_name)) \
            .toPandas()
        ax = dp.plot(x="date", y="avg_ft_" + col_name, figsize=(10, 5))
        ax.set_title(col_name)
        plt.show()


def plot_daily_count(path, start_date=None, end_date=None):
    """
    plot data by daily
    :param path:
    :param start_date: instance of datetime.date
    :param end_date: instance of datetime.date
    :return: pd.Dataframe: daily count
    """
    if start_date != None:
        lower_condition = "date >= '{}'".format(dt.strftime(start_date, "%Y-%m-%d"))
        start_date = start_date_path(path)
    else:
        lower_condition = "1 =1"
    if end_date != None:
        upper_condition = "date <= '{}'".format(dt.strftime(end_date, "%Y-%m-%d"))
        end_date = end_date_path(path)
    else:
        upper_condition = "1=1"

    df = spark.read.parquet(path) \
            .where(lower_condition) \
            .where(upper_condition)

    df_group = df.groupBy("date").agg(F.count("*").alias("count1"))


    date_range = spark.createDataFrame(pd.DataFrame(pd.date_range(start_date, end_date), columns=["date", ])) \
        .withColumn("date", F.to_date("date"))

    dp = date_range.join(df_group, "date", "outer") \
        .orderBy("date") \
        .fillna(0) \
        .toPandas()
    dp.plot(x="date", y="count1", kind="line", figsize=(20, 10))
    plt.show()
    return dp

def plot_daily_size(path, start_date=None, end_date=None):
    if start_date == None:
        start_date = start_date_path(path)
    if end_date == None:
        end_date = end_date_path(path)
    size_all = get_size(path)
    dp = pd.DataFrame(list(size_all.items()), columns=["date", "size"])
    dp['date'] = pd.to_datetime(dp["date"])
    date_range = pd.DataFrame(pd.date_range(start_date, end_date), columns=["date", ])
    dp = date_range.merge(dp, on="date", how= "outer").fillna(0)
    dp = dp.sort_values("date")
    dp.plot(x="date", y="size", figsize=(20, 8), kind="line")
    plt.show()
    return dp
