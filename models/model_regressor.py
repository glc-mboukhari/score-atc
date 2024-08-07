import pandas as pd
from catboost import CatBoostRegressor
from typing import Dict
import pickle, os

'''
class SecondaryModels:
    def __init__(self, models_paths: Dict[int, str]):
        self.models_paths = models_paths
        self.models = self._load_models()

    def _load_models(self) -> Dict[int, CatBoostClassifier]:
        models = {}
        for key, path in self.models_paths.items():
            model = CatBoostClassifier()
            model.load_model(path)
            models[key] = model
        return models
'''
class SecondaryModels:
    def __init__(self, models_paths: Dict[int, str]):
        self.models_paths = models_paths
        self.models = self._load_models()

    def _load_models(self) -> Dict[int, CatBoostRegressor]:
        models = {}
        for key, path in self.models_paths.items():
            print(path)
            if not os.path.exists(path):
                raise FileNotFoundError(f"The model file does not exist at the specified path: {path}")
            try:
                with open(path, 'rb') as file:
                    model = pickle.load(file)
                print(f"Model loaded successfully from {path}")
            except Exception as e:
                raise ValueError(f"Error loading model from {path}: {e}")
            models[key] = model
        return models

    def predict(self, model_key: int, data: pd.DataFrame) -> pd.Series:
        model = self.models.get(model_key)
        if model is None:
            raise ValueError(f"Model '{model_key}' not found.")
        return model.predict(data)

if __name__ == "__main__":
    models_paths = {
        0: "save_models/model_null.pkl",
        1: "save_models/model_nonull.pkl"
    }
    sm = SecondaryModels(models_paths=models_paths)
    print("Model Parameters 1:", sm.models[0].get_params())
    print("Model Parameters 2:", sm.models[1].get_params())