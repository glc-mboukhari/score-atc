import pandas as pd
from typing import List, Dict
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from load_model import ModelLoader
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.model_regressor import SecondaryModels
from models.model_lead_nolead import PrimaryModel
from processing.process.encoding import CategoryEncoder
import numpy as np

DYNAMO_TABLE_NAME = "TEST"
PROFILE_NAME = 'datascientist@test'

class ModelManager:
    def __init__(self, primary_model_path: str, secondary_models_paths: Dict[int, str], data:pd.DataFrame):
        self.primary_model = PrimaryModel(model_path=primary_model_path)
        self.secondary_models = SecondaryModels(models_paths=secondary_models_paths)
        self.region_name = 'us-east-1' 
        self.dynamodb_table_name = DYNAMO_TABLE_NAME
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region_name)
        self.table = self.dynamodb.Table(self.dynamodb_table_name)
        self.data = data
            
    def load_data(self) -> pd.DataFrame:
        '''
        Load data located in a specific location in s3
        :return pd.Dataframe 
        '''
        boto3.setup_default_session(profile_name=PROFILE_NAME)
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
        return pd.read_csv(obj['Body'])
    
    def remove_columns_inference(self):
        list_columns_to_remove = ['reference','date_snapshot','first_online_date','owner_correlation_id','vehicle_id','year','day','month','v_start_date','v_end_date','vehicle_internal_color','vehicle_external_color', 'options',
                          'vehicle_version_id','customer_type','vehicle_motorization','vehicle_version', 'zip_code',
                          'vehicle_first_circulation_date','constructor_warranty_end_date', 'vehicle_first_circulation_date_date','constructor_warranty_end_date_date','date_snapshot_date','vehicle_commercial_name','good_deal_badge_value','mileage_badge_value','ccl_type_de_bien',
                          'price','pictures_data_count','vehicle_rated_horse_power','manufacturer_warranty_duration','total_price_hors_option','vehicle_model','adjectives','motor_type','vehicle_make','selection_pack']
        self.data.drop(list_columns_to_remove,axis=1,inplace=True)

    def convert_to_category(self) :
        cats = self.data.select_dtypes(exclude=np.number).columns.tolist()
        for col in cats:
            self.data[col] = self.data[col].astype('category')

    def manage_encoding(self, data: pd.DataFrame) -> pd.DataFrame:
        self.primary_model.encoder

    def manage_prediction(self, data: pd.DataFrame) -> pd.DataFrame:
        # Get primary model predictions
        data_w_ref = data.copy()

        #ATTENTION ICI
        enc = self.primary_model.encoder
        data = enc.transform(data)
        data = self.remove_columns_inference(data)

        primary_predictions = self.primary_model.predict(data)
        data_w_ref['primary_prediction'] = primary_predictions

        
        data_0 = data[data['primary_prediction'] <= 0.5]
        data_not_0 = data[data['primary_prediction'] > 0.5]

        data_0_w_ref = data_0.copy()
        data_not_w_ref = data_not_0.copy()

        data_0 = self.remove_columns_inference(data_0)
        data_not_0 = self.remove_columns_inference(data_not_0)

        enc_0 = self.secondary_models.encoders[0]
        enc_1 = self.secondary_models.encoders[1]

        data_0 = enc_0.transform(data_0)
        data_not_0 = enc_1.transform(data_not_0)

        secondary_predictions_0 = self.secondary_models.models[0].predict(data_0)
        secondary_predictions_1 = self.secondary_models.models[1].predict(data_not_0)

        data_0_w_ref['secondary_predictions'] = secondary_predictions_0
        data_not_w_ref['secondary_predictions'] = secondary_predictions_1

        data_0_w_ref = data_0_w_ref[['reference','date_snapshot','secondary_predictions']]
        data_not_w_ref = data_not_w_ref[['reference','date_snapshot','secondary_predictions']]

        final_data = pd.concat([data_0_w_ref,data_not_w_ref],axis=0)
        return final_data
        
    def manage_prediction(self, data: pd.DataFrame) -> pd.DataFrame:
        # Get primary model predictions
        primary_predictions = self.primary_model.predict(data)

        # Use primary predictions to select the appropriate secondary model
        results = []
        for idx, prediction in enumerate(primary_predictions):
            # Determine which secondary model to use based on primary model prediction
            secondary_model_key = int(prediction)
            secondary_prediction = self.secondary_models.predict(secondary_model_key, data.iloc[[idx]])
            results.append(secondary_prediction[0])

        return pd.DataFrame(results, columns=['final_prediction'])

    def push_predictions_to_dynamodb(self, predictions: List[Dict[str, str]]) -> None:
        for item in predictions:
            try:
                self.table.put_item(Item=item)
            except (NoCredentialsError, PartialCredentialsError) as e:
                print(f"Credentials error: {e}")
            except Exception as e:
                print(f"Error pushing to DynamoDB: {e}")

if __name__ == "__main__":
    primary_model_path = "../models/save_models_training/model_lead_nolead.pkl"
    secondary_models_paths = {
        0: "../models/save_models_training/model_null.pkl",
        1: "../models/save_models_training/model_nonull.pkl"
    }

    # Load new data for prediction
    new_data = pd.read_csv("../data/data_inference.csv")
    new_data = new_data.loc[:, ~new_data.columns.str.contains('^Unnamed')]
    manager = ModelManager(primary_model_path=primary_model_path, secondary_models_paths=secondary_models_paths,data=new_data)
    manager.remove_columns_inference()
    manager.convert_to_category()
    final_predictions = manager.manage_prediction(new_data)
    print(final_predictions)
