import pandas as pd
from catboost import CatBoostClassifier
import pickle,os
'''
class PrimaryModel:
    def __init__(self, model_path: str):
        self.model_path = model_path
        self.model = CatBoostRegressor()
        self.model.load_model(self.model_path)

    def predict(self, data: pd.DataFrame) -> pd.Series:
        return self.model.predict(data)
'''
class PrimaryModel:
    def __init__(self, model_path: str):
        self.model_path = model_path
        
        if not os.path.exists(self.model_path):
            raise FileNotFoundError(f"The model file does not exist at the specified path: {self.model_path}")
        try:
            with open(self.model_path, 'rb') as file:
                self.model = pickle.load(file)
            print(f"Model loaded successfully from {self.model_path}")
        except Exception as e:
            raise ValueError(f"Error loading model from {self.model_path}: {e}")

    def predict(self, data: pd.DataFrame) -> pd.Series:
        return self.model.predict(data)

if __name__ == "__main__":
    model_path = 'save_models/model_lead_nolead.pkl'
    pm = PrimaryModel(model_path=model_path)
    # Check some properties of the model
    print("Model Parameters:", pm.model.get_params())
    print("Model Iterations:",  pm.model.tree_count_)
    print("Model Feature Names:",  pm.model.feature_names_)