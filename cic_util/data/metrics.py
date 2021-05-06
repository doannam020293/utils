from __future__ import print_function
from pyspark.sql import functions as F

def cal_psi(df1, df2, col_name, n_bin = 20, bin_type ="quantiles", verbose =False):
    """

    :param df1: dataframe
    :param df2: dataframe
    :param col_name: column to calculate psi
    :param n_bin: number bin
    :param bin_type: is "quantiles" or "bins"
    :param verbose: print explaination
    :return: psi
    """
    if bin_type not in ("quantiles", "bins"):
        print("need bin_type = quantiles or bins")
        raise ValueError
        return
    df_union = df1.select(col_name)\
        .unionByName(df2.select(col_name))
    if bin_type == "quantiles":
        delta = 1.0/float(n_bin)
        prob_list = [0.0 + delta * i for i in range(n_bin)]
        point_list = df_union.stat.approxQuantile(col_name, prob_list, 0.001)
    if bin_type == "bins":
        min_max = df_union\
            .select(F.min(col_name).alias("min"), F.max(col_name).alias("max"))\
            .collect()[0]
        min_value = min_max["min"]
        max_value = min_max["max"]
        delta = (max_value - min_value)/ float(n_bin)
        point_list = [min_value + delta * i for i in range(n_bin)]
    bin_expr = ""
    for i in range(len(point_list)):
        if i ==0:
            # first item only compare <
            col_value = "when {0} < {1} then {2} \n "\
                .format(col_name, point_list[i + 1], i + 1)
        elif (i == len(point_list) -1):
            # final item: only compare >=
            col_value = "when {0} > {1} then {2} \n "\
                        .format(col_name, point_list[i], i + 1)
        else:
            col_value = "when {0} between {1} and {2} then {3} \n "\
                .format(col_name, point_list[i], point_list[i + 1], i + 1)
        bin_expr += col_value
    # Null value bin =0, NAN value bin = max_bin.
    bin_expr = "case \n " + bin_expr + " else 0 \n end"
    if verbose:
        print("bin_expr",bin_expr)
        print("point_list",point_list)
    df1_count = df1.count()
    df1_agg = df1.withColumn("bin", F.expr(bin_expr))\
        .groupBy("bin")\
        .agg(
            (F.count("*")/df1_count).alias("count_per1"))
    df2_count = df2.count()
    df2_agg = df2.withColumn("bin", F.expr(bin_expr))\
        .groupBy("bin")\
        .agg(
            (F.count("*")/df2_count).alias("count_per2"))
    df_psi = df1_agg\
        .join(df2_agg, "bin", "outer")\
        .withColumn("psi", (F.expr("count_per1 - count_per2") *F.log(F.expr("count_per1/count_per2"))))
    psi = df_psi.select(F.sum("psi").alias("psi")).collect()[0]["psi"]
    return psi

