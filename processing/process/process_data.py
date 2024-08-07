from typing import Literal, Tuple, List
import pandas as pd
from collections import defaultdict
import spacy
from spacy.matcher import Matcher
import re
from langdetect import detect
from spacy.language import Language
import ast
import numpy as np

DEFAULT_VALUE_WHEN_NO_TRIM_LEVEL = ""


class ProcessDataForATC:
    def __init__(self, data:pd.DataFrame):
        self.data = data
        self.nlp_en = spacy.load("en_core_web_sm")
        self.nlp_fr = spacy.load("fr_core_news_sm")
        self.color_dict = {
        'blue': 'bleu', 'red': 'rouge', 'green': 'vert', 'yellow': 'jaune', 'orange': 'orange',
        'purple': 'violet', 'pink': 'rose', 'gray': 'gris', 'grey': 'gris',
        'black': 'noir', 'white': 'blanc', 'brown': 'marron',
        'bleu': 'bleu', 'rouge': 'rouge', 'vert': 'vert', 'jaune': 'jaune', 'orange': 'orange',
        'violet': 'violet', 'rose': 'rose', 'gris': 'gris', 'noir': 'noir', 'blanc': 'blanc',
        'marron': 'marron'
    }
        self.DEFAULT_VALUE_WHEN_NO_TRIM_LEVEL = ""
        self.num_variables = ['initial_price','vehicle_co2','price','vehicle_year','pictures_data_count_valid_photosphere','pictures_data_count_valid','vehicle_doors','vehicle_power_din',
                 'vehicle_rated_horse_power','vehicle_cubic','vehicle_trunk_volume','pictures_data_count_valid360_exterieur','vehicle_length',
                 'vehicle_height','vehicle_weight','vehicle_brut_quotation','good_deal_rate','vehicle_refined_quotation','vehicle_mileage','vehicle_average_mileage',
                 'vehicle_seats','pictures_data_count','nb_options','total_options_price','nb_ic','nb_listing','nb_detail','nb_place_parking','total_price','total_price_hors_option','vehicle_trim_level','network_warranty_duration','vehicle_owners']
        self.cat_variables = ['vehicle_gearbox','vehicle_make','vehicle_external_color','vehicle_energy','vehicle_first_hand','zip_code','vehicle_model','autoviza_display','vehicle_commercial_name'
                 ,'vehicle_internal_color','vehicle_version','vehicle_category','good_deal_badge','mileage_badge','vehicle_pollution_norm','vehicle_crit_air','vehicle_reliability_index','customer_type','mileage_badge_value','good_deal_badge_value','constructor_warranty_end_date',
                 'vehicle_condition','vehicle_motorization','vehicle_trim_level','options','ccl_type_de_bien','niveau_pack','selection_pack']

    def get_dataframe(self):
        return self.data
    
    def compute_trim_level(self) -> None:
        """
        For every trim-level (finition), it computes the median price_new of the finition scaled with respect to other
        finitions in a given commercial model.
            - trim_price_normalized = 1 means finition price that equals the mean price of the average price.
            - trim_price_normalized < 1 shows a very basic finition lower the average price.
            - trim_price_normalized > 1 shows a more expensive finition above the average price
        """
        group_commercial_model = ["vehicle_make", "vehicle_model", "vehicle_commercial_name"]
        group_trim_level = group_commercial_model + ["vehicle_trim_level"]

        # computes the trim_price (finition) median price and commercial model price
        self.data["vehicle_trim_level"] = self.data["vehicle_trim_level"].fillna(self.DEFAULT_VALUE_WHEN_NO_TRIM_LEVEL)

        df_trim_median_price = self.data.groupby(group_trim_level)["v_specs_price"].median().reset_index()
        df_trim_median_price = df_trim_median_price.rename({"v_specs_price": "trim_price"}, axis=1)
        df_comm_model_mean_price = self.data.groupby(group_commercial_model)["v_specs_price"].mean().reset_index()
        df_comm_model_mean_price = df_comm_model_mean_price.rename({"v_specs_price": "comm_model_price"}, axis=1)
        
        # get trim-price normalized
        df_trim_scaled_price = df_trim_median_price.merge(df_comm_model_mean_price, how="left", on=group_commercial_model)
        df_trim_scaled_price["trim_level_normalized"] = df_trim_scaled_price["trim_price"] / (df_trim_scaled_price["comm_model_price"] + 1e-6)
        df_trim_scaled_price["trim_level_normalized"] = df_trim_scaled_price["trim_level_normalized"].fillna(1.0) 

        dict_trim_level = df_trim_scaled_price.set_index(group_trim_level)["trim_level_normalized"].to_dict()
        dict_trim_level = defaultdict(lambda: 1.0, dict_trim_level)

        # store the trim_price_normalized as a default dict to handle unknown finitions
        self.data["vehicle_trim_level"] = self.data.apply(
            lambda row: dict_trim_level[(row[group_trim_level[0]], row[group_trim_level[1]], row[group_trim_level[2]], row[group_trim_level[3]])], axis=1
        )
    def clean_type_data(self) -> None:
        """
        Allow to clean data type of multiple numerical columns categorized as object
        """
        self.data = self.data.loc[:, ~self.data.columns.str.contains('^Unnamed')]
        object_columns = self.data[self.num_variables].select_dtypes(include=['object']).columns
        self.data[object_columns] = self.data[object_columns].astype(float)

    def treatment_bad_import(self) -> None:
        """
        Handle the case where the columns are badly delimited and imported. 
        """
        col_name = 'vehicle_rated_horse_power'
        self.data[col_name] = pd.to_numeric(self.data[col_name], errors='coerce')
        self.data = self.data.dropna(subset=[col_name])

    def clean_text_data(self) -> None:
        """
        Clean text data: lower values
        """
        str_columns = self.data.select_dtypes(exclude=['number']).columns
        self.data[str_columns] = self.data[str_columns].apply(lambda x: x.str.lower())
    
    def extract_first_two_digits(self, zip_code:str) -> str:
        """"
        Extract the department from zip code
        """
        return zip_code[:2]

    def detect_language(self, sentence:str) -> str:
        """
        Allow to detect the language of a string 
        """
        try:
            lang = detect(sentence)
            return lang
        except:
            return "Unknown"

    def extract_and_translate_colors(self, text:str) -> Tuple[str, str]:
        """
        
        """
        text = str(text)
        # Regular expression pattern to match common color names in English and French
        color_pattern = re.compile(r'\b(' + '|'.join(self.color_dict.keys()) + r')\b', flags=re.IGNORECASE)

        # Find all color matches in the text
        colors = color_pattern.findall(text)
        color = colors[0]

        # Translate colors to French
        translated_colors = [self.color_dict[color.lower()] for color in colors]
        translated_color = translated_colors[0]

        return color,translated_color
    

    def extract_adjectives_3plus(self, text:str) -> str:
        """
        
        """
        language = self.detect_language(text)
        adjectives = []
        # Load the spaCy model for the specified language
        if language == 'en':
            nlp = self.nlp_en
        elif language == 'fr':
            nlp = self.nlp_fr
        else:
            adjectives.append(text.split(' ')[0])
            return adjectives
        
        # Process the text using spaCy
        doc = nlp(text)

        # Create a matcher and define patterns for adjectives
        matcher = Matcher(nlp.vocab)
        
        # Basic pattern for adjectives
        adj_pattern = [{'POS': 'ADJ'}]
        
        # Compound adjectives pattern (e.g., adjectives joined by conjunctions)
        compound_adj_pattern = [
            {'POS': 'ADJ'}, {'POS': 'CCONJ'}, {'POS': 'ADJ'}
        ]
        
        # Add patterns to the matcher
        matcher.add('adjectives', [adj_pattern])
        matcher.add('compound_adjectives', [compound_adj_pattern])

        # List of known adjective endings in French (extendable as needed)
        adjective_endings = ['é', 'ée', 'és', 'ées', 'ant', 'ante', 'ants', 'antes', 'eux', 'euse', 'euses', 'if', 'ive', 'ifs', 'ives']

        # Custom rule to match adjectives with known endings
        custom_patterns = [[{'TEXT': {'REGEX': f'.*({ending})$'}}] for ending in adjective_endings]
        matcher.add('custom_adjectives', custom_patterns)

        # Find matches in the doc
        matches = matcher(doc)

        # Extract adjectives
        adjectives = [doc[start:end].text for match_id, start, end in matches]

        if len(adjectives) == 0:
            adjectives.append(text.split(' ')[0])

        return adjectives

    def process_color(self, text:str) -> Tuple[str, str]:
        """
        Allow to extract the color and the adjective (if exists) from a string
        3 cases: 
            - if the length of the split string is 1 then the word is the color 
            - if the length of the split string is 2 then one word is the color and the other is the adjective
            - if the length of the split string is 3 or more then we firt extract and remove the color, and we apply the function extracting the adjectives. 
        """
        if isinstance(text,str):
            list_text=  text.split(' ')
            if len(list_text) == 1:
                try:
                    color = self.color_dict[text]
                except:
                    color = list_text[0]
                adj = None
                return color,adj
            elif len(list_text) == 2:
                try:
                    color, translated_color = self.extract_and_translate_colors(text)
                except:
                    color, translated_color = list_text[0], list_text[0]
                adj = text.replace(color,'').strip()
                return translated_color,adj
            else:
                try:
                    color,translated_color = self.extract_and_translate_colors(text)
                except:
                    color, translated_color = list_text[0],list_text[0]
                adj = self.extract_adjectives_3plus(text.replace(color,'').strip())[0]
                return translated_color,adj
        else:
            return None,None
    
    def extract_motor_type(self,motorization:str) -> str:
        """
        Extract from the vehicle_motorization the motor_type
        """
        if pd.isnull(motorization):
            return None
        parts = motorization.split()
        if len(parts) != 3:
            return None
        return parts[1]

    def handle_opt(self,options:str) -> str:
        """
        function to keep the correctly formated options (as a list)
        """
        opt = str(options)
        if opt.startswith('[') and opt.endswith(']'):
            return options
        else:
            return '[]'
        
    def is_options(self,list_opt:List[str]) ->  int:
        """
        check if there are options or not according to options length
        """
        if len(list_opt):
            return 1
        else:
            return 0
        
    def handle_date(self) -> None:
        """
        Instead of keeping the dates (vehicle_first_circulation_date and constructor_warranty_end_date), we compute the time in days to the date_snapshot.
        For vehicle_first_circulation_date, we split the dataset where the difference computed is null and not null. When it's null, we use the vehicle_year 
        instead of the vehicle_first_circulation_date.
        """
        self.data['vehicle_first_circulation_date_date'] = pd.to_datetime(self.data['vehicle_first_circulation_date'])
        self.data['constructor_warranty_end_date_date'] = pd.to_datetime(self.data['constructor_warranty_end_date'])
        self.data['date_snapshot_date'] = pd.to_datetime(self.data['date_snapshot'])
        
        self.data['diff_warranty'] = (self.data['constructor_warranty_end_date_date'] - self.data['date_snapshot_date']).dt.days
        self.data['diff_circulation'] = (self.data['date_snapshot_date']- self.data['vehicle_first_circulation_date_date']).dt.days
        
        data1 = self.data[self.data['diff_circulation'].apply(lambda x: pd.notnull(x))]
        data2 = self.data[self.data['diff_circulation'].apply(lambda x: not(pd.notnull(x)))]
        data2['diff_circulation'] = (data2['date_snapshot_date'].apply(lambda x: x.year) - data2['vehicle_year'])*365
        
        data = pd.concat([data1,data2],axis=0) 
        self.data = data
    
    def encoding_variables(self) -> None:
        """
        Target (new_target:score atc) encoding of multiple categorical variables with a lot of category. 
        """
        target_mean_comm = self.data.groupby('vehicle_commercial_name')['new_target'].mean()
        self.data['vehicle_commercial_name_encoded'] = self.data['vehicle_commercial_name'].map(target_mean_comm)

        target_mean_model = self.data.groupby('vehicle_model')['new_target'].mean()
        self.data['vehicle_model_encoded'] = self.data['vehicle_model'].map(target_mean_model)

        target_mean_adj = self.data.groupby('adjectives')['new_target'].mean()
        self.data['vehicle_adj_encoded'] = self.data['adjectives'].map(target_mean_adj)

        target_mean_motor = self.data.groupby('motor_type')['new_target'].mean()
        self.data['vehicle_motor_encoded'] = self.data['motor_type'].map(target_mean_motor)

        target_mean_make = self.data.groupby('vehicle_make')['new_target'].mean()
        self.data['vehicle_make_encoded'] = self.data['vehicle_make'].map(target_mean_make)

    def create_boolean(self, nb:int) -> bool:
        """
        Create a boolean when the number (value) is > 0 (or not).
        """
        if nb > 0:
            return 1
        else:
            return 0

        
    def create_kpi_score(self):
        """
        Creation of some KPIs:
            - listing_to_ic: nb_ic / nb_listing
            - listing_to_detail: nb_detail / nb_listing
            - detail_to_ic: nb_ic / nb_detail
            - at_least_one_detail: if nb_detail > 0 =>  1
            - at_least_one_ic: if nb_ic > 0 =>  1 (target of one model)
            - nb_log_ic: log(1+IC) since IC is skewed positively
            - nb_log_detail: log(1+detail) since detail is skewed positively
            - nb_log_listing: log(1+listing) since listing is skewed positively
            - new_target: (100*log(1+IC)+0.5*log(1+detail))/(log(1+listing))
        """
        self.data['listing_to_ic'] = (self.data['nb_ic'] / self.data['nb_listing']).round(4)
        self.data['listing_to_detail'] = (df['nb_detail'] / self.data['nb_listing']).round(4)
        self.data['detail_to_ic'] = (self.data['nb_ic'] / self.data['nb_detail']).round(4)

        self.data['at_least_one_detail'] = self.data['nb_detail'].apply(lambda x: self.create_boolean(x))
        self.data['at_least_one_ic'] = self.data['nb_ic'].apply(lambda x: self.create_boolean(x))

        self.data['nb_log_ic'] = np.log1p(self.data['nb_ic'])
        self.data['nb_log_detail'] = np.log1p(self.data['nb_detail'])
        self.data['nb_log_listing'] = np.log1p(self.data['nb_listing'])

        self.data['new_target'] = (100*self.data['nb_log_ic'] + 0.5*self.data['nb_log_detail'])/(self.data['nb_log_listing'])

if __name__ == "__main__":
    from pathlib import Path

    def load_data(file_name):
        script_dir = Path(__file__).resolve().parent
        print(f"Script directory: {script_dir}")

        data_path = script_dir / '../../data' / file_name
        print(f"Constructed data path (before resolve): {data_path}")

        data_path = data_path.resolve()
        print(f"Resolved data path: {data_path}")

        # Load your data
        data = pd.read_csv(data_path)
        return data

    data = load_data('final_ann_nov_2023_april_2024.csv')
    
    process = ProcessDataForATC(data)
    process.compute_trim_level()
    process.treatment_bad_import()
    process.clean_type_data()
    process.clean_text_data()
    df = process.get_dataframe()[0:1000]
    
    df['dpt'] = df['zip_code'].apply(process.extract_first_two_digits)
    
    df[['color','adjectives']] = df['vehicle_external_color'].apply(lambda x: pd.Series(process.process_color(x)))
    
    df['motor_type'] = df['vehicle_motorization'].apply(lambda x: process.extract_motor_type(x))
    
    df['options'] = df['options'].apply(lambda x: process.handle_opt(x))
    df['options'] = df['options'].apply(ast.literal_eval)
    df['options'] = df['options'].apply(lambda x: process.is_options(x))

    process.data = df 

    process.handle_date()
    
    process.create_kpi_score()

    process.encoding_variables()

    df = process.get_dataframe()
    print(df.head())
