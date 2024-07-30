import pandas as pd
from catboost import CatBoostClassifier
from typing import Dict

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

    def predict(self, model_key: int, data: pd.DataFrame) -> pd.Series:
        model = self.models.get(model_key)
        if model is None:
            raise ValueError(f"Model '{model_key}' not found.")
        return model.predict(data)
