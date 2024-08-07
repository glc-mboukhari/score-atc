import pandas as pd
import boto3
from catboost import CatBoostRegressor
from typing import Dict

PROFILE_NAME = 'datascientist@test'

class TrainSecondaryModels:
    def __init__(self, s3_bucket: str, s3_key: str, model_save_paths: Dict[int, str]):
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.model_save_paths = model_save_paths

    def load_data(self) -> pd.DataFrame:
        boto3.setup_default_session(profile_name=PROFILE_NAME)
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
        return pd.read_csv(obj['Body'])

    def train_model(self, data: pd.DataFrame, save_path: str) -> None:
        X = data.drop(columns=['new_target'])
        y = data['new_target']
        model = CatBoostRegressor(iterations=1000, depth=6, learning_rate=0.1, loss_function='Logloss')
        model.fit(X, y)
        print('DONE')
        model.save_model(save_path)

    def train_all_models(self) -> None:
        data = self.load_data()
        data_0 = data[data['nb_ic'] == 0]
        data_not_0 = data[data['nb_ic'] != 0]

        self.train_model(data_0, self.model_save_paths[0])
        #self.train_model(data_not_0, self.model_save_paths[1])

if __name__ == "__main__":
    model_save_paths = {
        0: "../models/save_models_training/model_null.pkl",
        1: "../models/save_models_training/model_nonull.pkl"
    }
    trainer = TrainSecondaryModels(
        s3_bucket="cf-templates-19h19dxn1johs-eu-west-1",
        s3_key="final_ann_post_treatment.csv",
        model_save_paths=model_save_paths
    )
    df = trainer.load_data()
    print(df['new_target'])
    '''
    trainer.train_all_models()
    '''