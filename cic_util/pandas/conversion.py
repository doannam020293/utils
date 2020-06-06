
def fillna_object_df(df):
    """
    fill na dataframe pandas to
    :param df:
    :return:
    """
    for col_name in df.select_dtypes("object").columns:
        df[col_name] = df[col_name].fillna("")
    return df