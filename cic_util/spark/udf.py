from pyspark.sql.column import Column
from pyspark.sql.column import _to_java_column
from pyspark.sql.column import _to_seq
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

def hash_func(col):
    _string_length = sc._jvm.vn.cicdata.common.encrypt.HashUDF.getFun()
    return Column(_string_length.apply(_to_seq(sc, [col], _to_java_column)))
