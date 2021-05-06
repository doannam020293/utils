from sklearn.metrics import roc_auc_score
def cal_auc(dp, col_ft, col_label = "label", return_size = False):
    dp_select = dp[~dp[col_ft].isnull()]
    size = dp_select.shape[0]
    if size > 0:
        result = roc_auc_score(dp_select["label"], dp_select[col_ft])
    else:
        result = 0
    if return_size:
        return result, size
    else:
        return result