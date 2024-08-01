from typing import Literal, Tuple, List
import pandas as pd
from collections import defaultdict
import spacy
from spacy.matcher import Matcher
import re
from langdetect import detect
from spacy.language import Language
import ast

DEFAULT_VALUE_WHEN_NO_TRIM_LEVEL = ""
color_dict = {
        'blue': 'bleu', 'red': 'rouge', 'green': 'vert', 'yellow': 'jaune', 'orange': 'orange',
        'purple': 'violet', 'pink': 'rose', 'gray': 'gris', 'grey': 'gris',
        'black': 'noir', 'white': 'blanc', 'brown': 'marron',
        'bleu': 'bleu', 'rouge': 'rouge', 'vert': 'vert', 'jaune': 'jaune', 'orange': 'orange',
        'violet': 'violet', 'rose': 'rose', 'gris': 'gris', 'noir': 'noir', 'blanc': 'blanc',
        'marron': 'marron'
    }

class ProcessDataForATC:
    def __init__(self, data:pd.DataFrame):
        self.data = data
        self.nlp_en: Language = spacy.load("en_core_web_sm")
        self.nlp_fr: Language = spacy.load("fr_core_news_sm")

    def get_dataframe(self):
        return self.data
    
    def compute_trim_level(self) -> None:
        group_commercial_model = ["vehicle_make", "vehicle_model", "vehicle_commercial_name"]
        group_trim_level = group_commercial_model + ["vehicle_trim_level"]

        self.data["vehicle_trim_level"] = self.data["vehicle_trim_level"].fillna(DEFAULT_VALUE_WHEN_NO_TRIM_LEVEL)

        df_trim_median_price = self.data.groupby(group_trim_level)["v_specs_price"].median().reset_index()
        df_trim_median_price = df_trim_median_price.rename({"v_specs_price": "trim_price"}, axis=1)
        df_comm_model_mean_price = self.data.groupby(group_commercial_model)["v_specs_price"].mean().reset_index()
        df_comm_model_mean_price = df_comm_model_mean_price.rename({"v_specs_price": "comm_model_price"}, axis=1)
        
        df_trim_scaled_price = df_trim_median_price.merge(df_comm_model_mean_price, how="left", on=group_commercial_model)
        df_trim_scaled_price["trim_level_normalized"] = df_trim_scaled_price["trim_price"] / (df_trim_scaled_price["comm_model_price"] + 1e-6)
        df_trim_scaled_price["trim_level_normalized"] = df_trim_scaled_price["trim_level_normalized"].fillna(1.0) 

        dict_trim_level = df_trim_scaled_price.set_index(group_trim_level)["trim_level_normalized"].to_dict()
        dict_trim_level = defaultdict(lambda: 1.0, dict_trim_level)

        self.data["vehicle_trim_level"] = self.data.apply(
            lambda row: dict_trim_level[(row[group_trim_level[0]], row[group_trim_level[1]], row[group_trim_level[2]], row[group_trim_level[3]])], axis=1
        )
    
    def clean_text_data(self, txt:str) -> None:
        
        str_columns = self.data.select_dtypes(exclude=['numbers']).columns
        self.data[str_columns] = self.data[str_columns].apply(lambda x: x.str.lower())
    
    def extract_first_two_digits(zip_code:str):
        return zip_code[:2]

    def detect_language(sentence:str) -> str:
        try:
            lang = detect(sentence)
            return lang
        except:
            return "Unknown"

    def extract_and_translate_colors(text:str) -> Tuple[str, str]:
        color_dict = {
            'blue': 'bleu', 'red': 'rouge', 'green': 'vert', 'yellow': 'jaune', 'orange': 'orange',
            'purple': 'violet', 'pink': 'rose', 'gray': 'gris', 'grey': 'gris',
            'black': 'noir', 'white': 'blanc', 'brown': 'marron',
            'bleu': 'bleu', 'rouge': 'rouge', 'vert': 'vert', 'jaune': 'jaune', 'orange': 'orange',
            'violet': 'violet', 'rose': 'rose', 'gris': 'gris', 'noir': 'noir', 'blanc': 'blanc',
            'marron': 'marron'
            # Add more color translations as needed
        }

        text = str(text)
        # Regular expression pattern to match common color names in English and French
        color_pattern = re.compile(r'\b(' + '|'.join(color_dict.keys()) + r')\b', flags=re.IGNORECASE)

        # Find all color matches in the text
        colors = color_pattern.findall(text)
        color = colors[0]

        # Translate colors to French
        translated_colors = [color_dict[color.lower()] for color in colors]
        translated_color = translated_colors[0]

        return color,translated_color
    

    def extract_adjectives_3plus(self,text:str) -> str:
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

    def process_color(self, text,color_dict,nlp_fr,nlp_en):
        if isinstance(text,str):
            list_text=  text.split(' ')
            if len(list_text) == 1:
                try:
                    color = color_dict[text]
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
                adj = self.extract_adjectives_3plus(text.replace(color,'').strip(),nlp_fr,nlp_en)[0]
                return translated_color,adj
        else:
            return None,None
    
    def extract_motor_type(self,motorization:str) -> str:
        if pd.isnull(motorization):
            return None
        parts = motorization.split()
        if len(parts) != 3:
            return None
        return parts[1]

    def handle_opt(self,options:str) -> str:
        opt = str(options)
        if opt.startswith('[') and opt.endswith(']'):
            return options
        else:
            return '[]'
        
    def is_options(self,list_opt:List[str]) ->  int:
        if len(list_opt):
            return 1
        else:
            return 0
        
    def handle_date(self) -> None:
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

    data = load_data('final_ann_post_treatment.csv')[0:500]
    process = ProcessDataForATC(data)
    process.compute_trim_level()
    process.clean_text_data()
    process.extract_first_two_digits()
    process.process_color()
    process.extract_motor_type()
    process.handle_opt()
    process.is_options()
    process.handle_date()
    process.encoding_variables()
    df = process.get_dataframe()
    print(df.head())
