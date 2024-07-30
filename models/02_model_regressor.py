import pandas as pd
from catboost import CatBoostRegressor

class LeadModel:
    def __init__(self, model_path: str):
        self.model_path = model_path
        self.model = CatBoostRegressor()
        self.model.load_model(self.model_path)

    def predict(self, data: pd.DataFrame) -> pd.Series:
        return self.model.predict(data)
