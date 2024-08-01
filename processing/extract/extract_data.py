from scripts.redshift.redshift_client import RedshiftExecutor
import pandas as pd 

class ExtractDataFromRedshift:
    def __init__(self,table_name, database,user,password,host,port) -> None:
        self.table_name = table_name
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def extract_data_from_redshift(self) -> pd.DataFrame:
        query = f'SELECT * FROM public.{self.table_name} LIMIT 10'
        red_client = RedshiftExecutor(self.database,self.user,self.password,self.host,self.port)
        red_client.connect()
        data = red_client.fetch_results(query)
        print(data)
        red_client.close()
        return data
    
    def create_tables_redshift(self,path_to_query:str) -> None:
        red_client = RedshiftExecutor(self.database,self.user,self.password,self.host,self.port)
        red_client.connect()
        red_client.execute_sql_file(path_to_query)
        red_client.close()

if __name__ == "__main__":
    extractor = ExtractDataFromRedshift('ann_online_nov_2023_april_2024',
                                        'dwhstats',
                                        'mboukhari',
                                        'b0!a*{UZ+7_"55if)zED',
                                        'stats-dwh-redsh.prod.carboat.cloud',
                                        '5439')
    extractor.extract_data_from_redshift()



'''
a = RedShiftExecutor('dwhstats','mboukhari', 'b0!a*{UZ+7_"55if)zED','stats-dwh-redsh.prod.carboat.cloud','5439')
a.connect()
with open(file_path, 'r') as file:
        sql_commands = file.read()

'''
