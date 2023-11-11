from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, roc_auc_score, recall_score, precision_score


class Model:

    def __init__(self):
        self.model = RandomForestClassifier()

    def fit(self, X_train, y_train):
        self.model.fit(X_train, y_train)

    def predict(self, X_test):
        return self.model.predict(X_test)

    def get_metrics(self, X_test, y_test):
        predict = self.model.predict(X_test)
        predict_proba = self.model.predict_proba(X_test)[:,1]

        d = {"F1_score": f1_score(y_test, predict),
             "Recall": recall_score(y_test, predict),
             "Precision": precision_score(y_test, predict),
             "ROC_AUC": roc_auc_score(y_test, predict_proba)}

        return d
