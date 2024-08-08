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
import numpy as np

DYNAMO_TABLE_NAME = "TEST"

class ModelManager:
    def __init__(self, primary_model_path: str, secondary_models_paths: Dict[int, str], data:pd.DataFrame):
        self.primary_model = PrimaryModel(model_path=primary_model_path)
        self.secondary_models = SecondaryModels(models_paths=secondary_models_paths)
        self.region_name = 'us-east-1' 
        self.dynamodb_table_name = DYNAMO_TABLE_NAME
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region_name)
        self.table = self.dynamodb.Table(self.dynamodb_table_name)
        self.data = data
            
    def remove_columns_inference(self):
        list_columns_to_remove = ['reference','date_snapshot','first_online_date','owner_correlation_id','vehicle_id','year','day','month','v_start_date','v_end_date','vehicle_internal_color','vehicle_external_color', 'options',
                          'vehicle_version_id','customer_type','vehicle_motorization','vehicle_version', 'zip_code',
                          'vehicle_first_circulation_date','constructor_warranty_end_date', 'vehicle_first_circulation_date_date','constructor_warranty_end_date_date','date_snapshot_date','vehicle_commercial_name','good_deal_badge_value','mileage_badge_value','ccl_type_de_bien',
                          'price','pictures_data_count','vehicle_rated_horse_power','manufacturer_warranty_duration','total_price_hors_option','vehicle_model','adjectives','motor_type','vehicle_make','selection_pack']
        self.data.drop(list_columns_to_remove,axis=1,inplace=True)
    
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
    primary_model_path = "../models/save_models/model_lead_nolead.pkl"
    secondary_models_paths = {
        0: "../models/save_models/model_null.pkl",
        1: "../models/save_models/model_nonull.pkl"
    }

    # Load new data for prediction
    new_data = pd.read_csv("../data/data_inference.csv")
    new_data = new_data.loc[:, ~new_data.columns.str.contains('^Unnamed')]
    manager = ModelManager(primary_model_path=primary_model_path, secondary_models_paths=secondary_models_paths,data=new_data)
    manager.remove_columns_inference()
    cats = new_data.select_dtypes(exclude=np.number).columns.tolist()
    for col in cats:
        manager.data[col] = manager.data[col].astype('category')
    final_predictions = manager.manage_prediction(new_data)
    print(final_predictions)
