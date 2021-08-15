from __future__ import print_function
from datetime import date, timedelta
from datetime import datetime as dt

from etl.common.utils import get_credentials
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window


spark = SparkSession.builder.getOrCreate()


def head_df(df, n = 10):
    return df.limit(n).toPandas().head(n)

def rparquet(path):
    """
    short funtion for read parquet
    :param path:
    :return:
    """
    return spark.read.parquet(path)


def cal_lift_table(df,col_score = "credit_scre", col_label = "label_value", n_bin = 10, ):
    """
    short funtion for read parquet
    :param path:
    :return:
    """
    df.groupBy(F.ntile(n_bin).over(Window.orderBy(F.desc(col_score))).alias("bin"))\
        .agg(F.count("*").alias("count"),
             F.min(col_score).alias(f"min_{col_score}"),
             F.max(col_score).alias(f"max_{col_score}"),
             F.bround(F.avg(col_label)*100.0, 2).alias(f"{col_label}_rate"))\
        .orderBy("bin").show(n_bin)
    return

def _wsql(df, host, port, db, username, password, dbtable_new, mode = "append",  n_partition =None):
    """
    write dataframe to database
    :param df:
    :param host:
    :param port:
    :param db:
    :param username:
    :param password:
    :param dbtable_new:
    :param mode:
    :param n_partition:
    :return:
    """
    if n_partition is not None:
        df = df.repartition(n_partition)
    df\
        .write.mode(mode).format("jdbc").options(
        url="jdbc:mysql://{}:{}/{}".format(host, port, db),
        driver="com.mysql.jdbc.Driver",
        dbtable=dbtable_new,
        user=username,
        password=password,
        batchsize=1000,
        rewriteBatchedStatements=True
        ).save()
    return


def wsql(df, credential, dbtable_new, mode="append", n_partition=None):
    """
    write dataframe to mysql with config from credential
    :param df:
    :param credential:
    :param dbtable_new:
    :param mode:
    :param n_partition:
    :return:
    """
    host, port, db, username, password = get_credentials(credential)
    _wsql(df, host, port, db, username, password, dbtable_new, mode, n_partition)
    return


def get_script_name(file_name):
    return file_name.split("/")[-1].replace(".py","")