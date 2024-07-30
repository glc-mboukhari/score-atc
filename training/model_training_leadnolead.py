import catboost
import pandas as pd
from catboost import CatBoostRegressor
from typing import Tuple
import boto3

class ModelTrainer:
    def __init__(self, model_save_path: str):
        self.model_save_path = model_save_path

    def load_data(self) -> pd.DataFrame:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
        return pd.read_csv(obj['Body'])

    def train(self) -> None:
        data = self.load_data()
        X = data.drop(columns=['target'])
        y = data['target']
        model = CatBoostRegressor(iterations=1000, depth=6, learning_rate=0.1, loss_function='Logloss')
        model.fit(X, y)
        model.save_model(self.model_save_path)


    def save_model(self, model: CatBoostRegressor) -> None:
        model.save_model(self.model_save_path)

