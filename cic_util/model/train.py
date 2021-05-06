
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import pandas as pd
import numpy as np
from sklearn.model_selection import cross_val_score



def train(all_train_data, ft_names=None, skip_col=None,label_col = "label", cross_val=True, print_corr=True):
    object_col = all_train_data.select_dtypes(include=["object", ]).columns.tolist()
    if skip_col == None:
        skip_col = ["phone_number", "local_id", "disburse_date", "date", "sex", "replied", "label_col", label_col, "label_value", "month",
                    "is_from_ts", "source", "send_time"] \
                   + object_col
    if ft_names == None:
        ft_names = [f for f in all_train_data.columns if f not in skip_col]

    X = all_train_data[ft_names].values
    Y = all_train_data[label_col]
    n_label_1 = np.sum(all_train_data[label_col])
    print("shape", all_train_data.shape)
    print("number label 1 ", n_label_1, "bad rate", np.round(n_label_1/all_train_data.shape[0]*100.0, 2))
    seed = 7
    test_size = 0.3
    train_data, test_data, train_labels, test_labels = train_test_split(X, Y, test_size=0.3)
    print(train_data.shape, test_data.shape)

    def get_feature_important(model, columns):
        a = list(model.feature_importances_)
        df_feats_importance = pd.DataFrame.from_dict({"Feature": columns, "Score": a})
        df_feats_importance.set_index(("Feature"), drop=True, inplace=True)
        df_feats_importance = df_feats_importance.sort_values(by="Score", ascending=False)
        return df_feats_importance

    model = XGBClassifier(nthread=32)
    model.fit(train_data, train_labels)

    predict_labels_prob = model.predict_proba(test_data)[:, 1]
    auc = roc_auc_score(test_labels, predict_labels_prob)
    print(auc)
    df_cor = get_feature_important(model, all_train_data[ft_names].columns.tolist())
    df_cor = df_cor.reset_index()
    print(df_cor.head())
    if cross_val:
        model = XGBClassifier(nthread=32)
        scores = cross_val_score(model, X, Y, cv=5, scoring="roc_auc")
        scores = [np.round(a*100, 2) for a in scores]
        print("auc cross validate",scores)
        print("mean auc", np.mean(scores), "max - min", np.max(scores) - np.min(scores), "std", np.std(scores) )
    if print_corr:
        top_ft = df_cor["Feature"].tolist()[:50]
        for col_name in top_ft:
            print(col_name, all_train_data[col_name].corr(all_train_data[label_col]))
    return df_cor
