import category_encoders as ce
import pandas as pd


class CategoryEncoder:
    def __init__(self):
        self.encoder_mapping_by_col = {}
        self.encoder = ce.leave_one_out.LeaveOneOutEncoder()

    def cache_encoder(self) -> None:
        self.encoder_mapping_by_col = {}
        for col, colmap in self.encoder.mapping.items():
            self.encoder_mapping_by_col[col] = (colmap["sum"] / colmap["count"]).where(colmap["count"] > 1, self.encoder._mean).to_dict()


    def transform(self, x_processed: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the encoder on each column of the dataframe fitted in the encoder
        :param x_processed: (dataframe) containing the features processed (already processed) but before encoding.
        :return: (dataframe) x_encoded where features have been encoded.
        """
        for col, mapping in self.encoder_mapping_by_col.items():
            x_processed[col] = x_processed[col].map(mapping.get).fillna(self.encoder._mean)

        return x_processed

    def fit(self, x_processed: pd.DataFrame, y_train: pd.DataFrame) -> "CategoryEncoder":
        """
        Fits the encoder.
        :param x_processed: (dataframe) containing the features processed (already processed) but before encoding.
        :param y_train: (dataframe) containing Y already prepared.
        :return: (no return) the encoder is assigned to the attributes of the estimator.
        """
        print("Fit encoder in progress...")
        x_to_encode = x_processed.copy()
        self.encoder.fit(x_to_encode, y_train)
        print("Fit encoder done.")

        print("Cache the encoder mappings...")
        self.cache_encoder()

        return self
    
if __name__ == "__main__":
    df = pd.read_csv('../../data/data_training.csv')
    col = ['vehicle_make','color']
    x_processed = df[col]
    y_train = df['nb_ic']
    a = CategoryEncoder().fit(x_processed,y_train)
    print(a.encoder_mapping_by_col)