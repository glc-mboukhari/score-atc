import pandas as pd
from typing import List, Dict
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from load_model import ModelLoader
import pandas as pd
from models.primary_model import PrimaryModel
from models.secondary_models import SecondaryModels

class ModelManager:
    def __init__(self, primary_model_path: str, secondary_models_paths: Dict[int, str]):
        self.primary_model = PrimaryModel(model_path=primary_model_path)
        self.secondary_models = SecondaryModels(models_paths=secondary_models_paths)
        self.dynamodb_table_name = dynamodb_table_name
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(dynamodb_table_name)

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
    primary_model_path = "models/primary_model.cbm"
    secondary_models_paths = {
        0: "models/secondary_model_0.cbm",
        1: "models/secondary_model_1.cbm"
    }

    # Load new data for prediction
    new_data = pd.read_csv("data/new_data.csv")

    manager = ModelManager(primary_model_path=primary_model_path, secondary_models_paths=secondary_models_paths)
    final_predictions = manager.manage_prediction(new_data)
    print(final_predictions)
