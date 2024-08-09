import catboost
import pandas as pd
from catboost import CatBoostClassifier
from typing import Tuple
import boto3
import numpy as np
from sklearn.model_selection import  train_test_split
import pickle
import os,sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processing.process.encoding import CategoryEncoder

PROFILE_NAME = 'datascientist@test'

class TrainPrimaryModel:
    def __init__(self, s3_bucket: str, s3_key: str,model_save_path: str):
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.model_save_path = model_save_path

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
    def assign_value(self,val:int) -> bool:
        if val > 0:
            return 1
        else:
            return 0
    def create_kpis_primarymodel(self,data: pd.DataFrame) -> pd.DataFrame:
        '''
        create binary variable to predict for the first model (lead or not lead)
        '''
        data['at_least_one_ic'] = data['nb_ic'].apply(lambda x: self.assign_value(x))
        # to compute in order to encode
        data['nb_log_ic'] = np.log1p(data['nb_ic'])
        data['nb_log_detail'] = np.log1p(data['nb_detail'])
        data['nb_log_listing'] = np.log1p(data['nb_listing'])

        data['new_target'] = (100*data['nb_log_ic'] + 0.5*data['nb_log_detail'])/(data['nb_log_listing'])
        return data

    def remove_columns_primarymodel(self,data: pd.DataFrame) -> pd.DataFrame:
        '''
        Columns to remoave due to high correlations, irrelevance.
        :params data (dataframe): dataframe containing ads to train
        :return data (dtaframe): 
        '''
        list_columns_to_remove = ['reference','date_snapshot','first_online_date','owner_correlation_id','vehicle_id','year','day','month','v_start_date','v_end_date','vehicle_internal_color','vehicle_external_color', 'options',
                          'vehicle_version_id','customer_type','vehicle_motorization','vehicle_version', 'zip_code',
                          'vehicle_first_circulation_date','constructor_warranty_end_date', 'vehicle_first_circulation_date_date','constructor_warranty_end_date_date','date_snapshot_date','vehicle_commercial_name','good_deal_badge_value','mileage_badge_value','ccl_type_de_bien',
                          'price','pictures_data_count','vehicle_rated_horse_power','manufacturer_warranty_duration','total_price_hors_option','vehicle_model','adjectives','motor_type','vehicle_make','selection_pack','nb_ic','nb_detail','nb_listing', 'nb_log_listing','nb_log_detail','nb_log_ic']
        data.drop(list_columns_to_remove,axis=1,inplace=True)
        return data

    def train(self, data:pd.DataFrame) -> CatBoostClassifier:
        '''
        Train first model using all the data to predict the power of an ad to have a lead or not.
        :param data (dataframe); training data with all ads to predict a boolean columns (lead or not_lead)
        :return model (Catboostclassifier): return the model that will be saved to predict. 
        '''
        X = data.drop(columns=['at_least_one_ic'])
        y = data['at_least_one_ic']
        cats = X.select_dtypes(exclude=np.number).columns.tolist()
        for col in cats:
            X[col] = X[col].astype('category')
        X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.7, shuffle=True, random_state=1)
        model = CatBoostClassifier(iterations=1000, depth=6, learning_rate=0.1, loss_function='Logloss', cat_features=cats)
        model.fit(X_train, y_train)
        return model
    
    def save_information(self, save_path:str, model:CatBoostClassifier, encoder: CategoryEncoder) -> None:
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
    
    def model_evaluation(self):
        pass

if __name__ == "__main__":
    model_save_path = "../models/save_models_training/model_training_lead_nolead.pkl"
    trainer = TrainPrimaryModel(
        s3_bucket="cf-templates-19h19dxn1johs-eu-west-1",
        s3_key="data_training.csv",
        model_save_path=model_save_path
    )
    df = trainer.load_data()
    df = trainer.business_filter(df)
    df = trainer.create_kpis_primarymodel(df)

    encoder = trainer.fit_encoding_training_data(df)
    df = encoder.transform(df)

    print('------')
    print(df.shape)
    df = trainer.remove_columns_primarymodel(df)
    print('------------')
    print(df.shape)
    model_lead_nolead = trainer.train(df)
    trainer.save_information(model_save_path, model_lead_nolead, encoder)
