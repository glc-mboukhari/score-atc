import pandas as pd
import boto3
from catboost import CatBoostClassifier
from typing import Dict

class TrainSecondaryModels:
    def __init__(self, s3_bucket: str, s3_key: str, model_save_paths: Dict[int, str]):
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.model_save_paths = model_save_paths

    def load_data(self) -> pd.DataFrame:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
        return pd.read_csv(obj['Body'])

    def train_model(self, data: pd.DataFrame, save_path: str) -> None:
        X = data.drop(columns=['target'])
        y = data['target']
        model = CatBoostClassifier(iterations=1000, depth=6, learning_rate=0.1, loss_function='Logloss')
        model.fit(X, y)
        model.save_model(save_path)

    def train_all_models(self) -> None:
        data = self.load_data()
        data_0 = data[data['nb_ic'] == 0]
        data_not_0 = data[data['nb_ic'] != 0]

        self.train_model(data_0, self.model_save_paths[0])
        self.train_model(data_not_0, self.model_save_paths[1])

if __name__ == "__main__":
    model_save_paths = {
        0: "models/secondary_model_0.cbm",
        1: "models/secondary_model_1.cbm"
    }

    trainer = TrainSecondaryModels(
        s3_bucket="your-s3-bucket",
        s3_key="path/to/train_data.csv",
        model_save_paths=model_save_paths
    )
    trainer.train_all_models()
