import pandas as pd
import boto3
from catboost import CatBoostRegressor
from typing import Dict, Tuple
import numpy as np
from sklearn.model_selection import  train_test_split
import os,sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processing.process.encoding import CategoryEncoder
import pickle


PROFILE_NAME = 'datascientist@test'

class TrainSecondaryModels:
    def __init__(self, s3_bucket: str, s3_key: str, model_save_paths: Dict[int, str]):
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.model_save_paths = model_save_paths


    def load_data(self) -> pd.DataFrame:
        '''
        Load data located in a specific location in s3
        :return pd.Dataframe 
        '''
        boto3.setup_default_session(profile_name=PROFILE_NAME)
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
        return pd.read_csv(obj['Body'])

    def business_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        '''
        Filters applied to ensure a better training of the dataset
            - part have twice IC because of pro contacting them
            - IC > 20 is very rare and can biais the model with score too high
            - listing = 0: ads blocked the day they were online
            - detail = 0: imply that IC is null and the score will be null
        :param data: (dataframe) containing the data post-processing 
        :return data: (dataframe) filtered data ready for training
        '''
        data = data.loc[:, ~data.columns.str.contains('^Unnamed')]
        data = data[data['customer_type']=='pro']
        data = data[data['nb_listing']!=0] 
        data = data[data['nb_detail']!=0]
        data = data[data['nb_ic']<20]
        ref = data[(data['nb_ic']==1) & (data['nb_detail']==1) & (data['nb_listing']==1)]['reference']
        data = data[~data['reference'].isin(ref)]
        return data
    
    def fit_encoding_training_data(self, data: pd.DataFrame) -> "CategoryEncoder":
        '''
        During training, allow to fit a new encoder on the training data.
        :param data (dataframe): Dataframe containing ads before all the pre-training processing.
        :return encoder (CategoryEncoder): return the instance of encoder after being fit
        '''
        col_to_encode = ['vehicle_commercial_name','vehicle_model','adjectives','motor_type','vehicle_make']
        target = 'new_target'         
        encoder = CategoryEncoder().fit(data[col_to_encode], data[target])
        return encoder

    def train_model(self, data: pd.DataFrame) -> CatBoostRegressor:
        '''
        Split the dataset as X and y in order to train it using a catboost regressor. The model is then saved in save_path.
        :param data (dataframe): data to train
        '''
        X = data.drop(columns=['new_target'])
        y = data['new_target']
        cats = X.select_dtypes(exclude=np.number).columns.tolist()
        for col in cats:
            X[col] = X[col].astype('category')
        X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.7, shuffle=True, random_state=1)
        model = CatBoostRegressor(iterations=100, depth=6, learning_rate=0.1, loss_function='MAPE',cat_features=cats)
        model.fit(X_train, y_train)
        return model
    
    def save_information(self, save_path:str, model:CatBoostRegressor, encoder: CategoryEncoder) -> None:
        '''
        Allow to save in a dictionary the model and the encoding class. It allows to access for training directly for training and prediction. 
        :params save_path, model, encoder:
        '''
        model_package = {
            'model' : model,
            'encoding_class': encoder
        }
        with open(save_path,'wb') as f:
            pickle.dump(model_package,f)

    def model_evaluation(self, model:CatBoostRegressor):
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
        '''
        Columns to remove due to high correlations, irrelevance.
        :params data (dataframe): dataframe containing ads to train
        :return data (dataframe): 
        '''
        list_columns_to_remove = ['reference','date_snapshot','first_online_date','owner_correlation_id','vehicle_id','year','day','month','v_start_date','v_end_date','vehicle_internal_color','vehicle_external_color', 'options',
                          'vehicle_version_id','customer_type','vehicle_motorization','vehicle_version', 'zip_code',
                          'vehicle_first_circulation_date','constructor_warranty_end_date', 'vehicle_first_circulation_date_date','constructor_warranty_end_date_date','date_snapshot_date','vehicle_commercial_name','good_deal_badge_value','mileage_badge_value','ccl_type_de_bien',
                          'price','pictures_data_count','vehicle_rated_horse_power','manufacturer_warranty_duration','total_price_hors_option','vehicle_model','adjectives','motor_type','vehicle_make','selection_pack','nb_log_listing','nb_log_detail','nb_log_ic','nb_ic','nb_detail','nb_listing']
        data.drop(list_columns_to_remove,axis=1,inplace=True)
        return data

    def create_df_training(self,data:pd.DataFrame) -> Tuple[pd.DataFrame,pd.DataFrame]:
        '''
        Split the dataset into one df where nb_ic = 0 and where nb_ic != 0. Two separate trainings are then done on each dataset
        :param data (dataframe): dataset containing the ads to train
        :return data_0, data_not_0 (dataframe, dataframe): data_0 (dataset nb_ic = 0), data_not_0 (dataset nb_ic != 0)
        '''
        data_0 = data[data['nb_ic'] == 0]
        data_not_0 = data[data['nb_ic'] != 0]
        return data_0, data_not_0
    
    def train_all_models(self, data_0: pd.DataFrame, data_not_0: pd.DataFrame) -> Tuple[CatBoostRegressor, CatBoostRegressor]:
        """
        Train both models and save them
        :param data_0 (dataframe), data_not_0 (dataframe): datasets where nb_ic = 0 and where nb_ic != 0
        """
        model_null = self.train_model(data_0)
        model_notnull = self.train_model(data_not_0)
        return model_null, model_notnull

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
    
    encoder = trainer.fit_encoding_training_data(df)
    df = encoder.transform(df)
    
    df1, df2 = trainer.create_df_training(df)
    df1 = trainer.remove_columns_secondarymodels(df1)
    df2 = trainer.remove_columns_secondarymodels(df2)

    model_null, model_notnull = trainer.train_all_models(df1,df2)

    trainer.save_information(model_save_paths[0], model_null, encoder)
    trainer.save_information(model_save_paths[1], model_notnull, encoder)
