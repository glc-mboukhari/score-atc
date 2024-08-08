import catboost
import pandas as pd
from catboost import CatBoostClassifier
from typing import Tuple
import boto3

class TrainPrimaryModel:
    def __init__(self, model_save_path: str):
        self.model_save_path = model_save_path

    def load_data(self) -> pd.DataFrame:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
        return pd.read_csv(obj['Body'])

    def create_kpis_primarymodel(self):
        '''
        create binary variable to predict for the first model (lead or not lead)
        '''
        self.data['at_least_one_ic'] = self.data['nb_ic'].apply(lambda x: self.create_boolean(x))

    def remove_columns_primarymodel(self):
        list_columns_to_remove = ['reference','date_snapshot','first_online_date','owner_correlation_id','vehicle_id','year','day','month','v_start_date','v_end_date','vehicle_internal_color','vehicle_external_color', 'options',
                          'vehicle_version_id','customer_type','vehicle_motorization','vehicle_version', 'zip_code',
                          'vehicle_first_circulation_date','constructor_warranty_end_date', 'vehicle_first_circulation_date_date','constructor_warranty_end_date_date','date_snapshot_date','vehicle_commercial_name','good_deal_badge_value','mileage_badge_value','ccl_type_de_bien',
                          'price','pictures_data_count','vehicle_rated_horse_power','manufacturer_warranty_duration','total_price_hors_option','vehicle_model','adjectives','motor_type','vehicle_make','selection_pack']
        self.data.drop(list_columns_to_remove,axis=1,inplace=True)

    def train(self) -> None:
        data = self.load_data()
        X = data.drop(columns=['new_target'])
        y = data['new_target']
        model = CatBoostClassifier(iterations=1000, depth=6, learning_rate=0.1, loss_function='Logloss')
        model.fit(X, y)
        model.save_model(self.model_save_path)

    def save_model(self, model: CatBoostClassifier) -> None:
        model.save_model(self.model_save_path)

