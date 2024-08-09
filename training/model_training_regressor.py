import pandas as pd
import boto3
from catboost import CatBoostRegressor
from typing import Dict, Tuple
import numpy as np
from sklearn.model_selection import  train_test_split

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

    def business_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        '''
        '''
        data = data.loc[:, ~data.columns.str.contains('^Unnamed')]
        data = data[data['customer_type']=='pro']
        data = data[data['nb_listing']!=0]
        data = data[data['nb_detail']!=0]
        data = data[data['nb_ic']<20]
        ref = data[(data['nb_ic']==1) & (data['nb_detail']==1) & (data['nb_listing']==1)]['reference']
        data = data[~data['reference'].isin(ref)]
        return data
    
    def train_model(self, data: pd.DataFrame, save_path: str) -> None:
        X = data.drop(columns=['new_target'])
        y = data['new_target']
        cats = X.select_dtypes(exclude=np.number).columns.tolist()
        for col in cats:
            X[col] = X[col].astype('category')
        X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.7, shuffle=True, random_state=1)
        model = CatBoostRegressor(iterations=1000, depth=6, learning_rate=0.1, loss_function='MAPE',cat_features=cats)
        model.fit(X_train, y_train)
        model.save_model(save_path)

    def model_evaluation(self):
        pass

    def create_kpis_secondarymodels(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Creation of some KPIs if we are in our training pipeline:
            - nb_log_ic: log(1+IC) since IC is skewed positively
            - nb_log_detail: log(1+detail) since detail is skewed positively
            - nb_log_listing: log(1+listing) since listing is skewed positively
            - new_target: (100*log(1+IC)+0.5*log(1+detail))/(log(1+listing))
        """
        data['nb_log_ic'] = np.log1p(data['nb_ic'])
        data['nb_log_detail'] = np.log1p(data['nb_detail'])
        data['nb_log_listing'] = np.log1p(data['nb_listing'])

        data['new_target'] = (100*data['nb_log_ic'] + 0.5*data['nb_log_detail'])/(data['nb_log_listing'])
        return data

    def remove_columns_secondarymodels(self,data: pd.DataFrame) -> pd.DataFrame:
        list_columns_to_remove = ['reference','date_snapshot','first_online_date','owner_correlation_id','vehicle_id','year','day','month','v_start_date','v_end_date','vehicle_internal_color','vehicle_external_color', 'options',
                          'vehicle_version_id','customer_type','vehicle_motorization','vehicle_version', 'zip_code',
                          'vehicle_first_circulation_date','constructor_warranty_end_date', 'vehicle_first_circulation_date_date','constructor_warranty_end_date_date','date_snapshot_date','vehicle_commercial_name','good_deal_badge_value','mileage_badge_value','ccl_type_de_bien',
                          'price','pictures_data_count','vehicle_rated_horse_power','manufacturer_warranty_duration','total_price_hors_option','vehicle_model','adjectives','motor_type','vehicle_make','selection_pack','nb_log_listing','nb_log_detail','nb_log_ic','nb_ic','nb_detail','nb_listing']
        data.drop(list_columns_to_remove,axis=1,inplace=True)
        return data

    def create_df_training(self,data:pd.DataFrame) -> Tuple[pd.DataFrame,pd.DataFrame]:
        data_0 = data[data['nb_ic'] == 0]
        data_not_0 = data[data['nb_ic'] != 0]
        return data_0, data_not_0
    
    def train_all_models(self, data_0: pd.DataFrame, data_not_0: pd.DataFrame) -> None:
        self.train_model(data_0, self.model_save_paths[0])
        #self.train_model(data_not_0, self.model_save_paths[1])

if __name__ == "__main__":
    model_save_paths = {
        0: "../models/save_models_training/model_training_null.pkl",
        1: "../models/save_models_training/model_training_nonull.pkl"
    }
    trainer = TrainSecondaryModels(
        s3_bucket="cf-templates-19h19dxn1johs-eu-west-1",
        s3_key="data_training.csv",
        model_save_paths=model_save_paths
    )
    df = trainer.load_data()
    df = trainer.business_filter(df)
    df = trainer.create_kpis_secondarymodels(df)
    print('------')
    print(df.shape)
    df1, df2 = trainer.create_df_training(df)
    df1 = trainer.remove_columns_secondarymodels(df1)
    df2 = trainer.remove_columns_secondarymodels(df2)
    print('------------')
    print(df1.shape)
    print(df2.shape)
    trainer.train_all_models(df1,df2)
