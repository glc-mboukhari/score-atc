import catboost
from catboost import CatBoostClassifier

class ModelLoader:
    def __init__(self, model_path: str):
        self.model_path = model_path

    def load_model(self) -> CatBoostClassifier:
        model = CatBoostClassifier()
        model.load_model(self.model_path)
        return model
