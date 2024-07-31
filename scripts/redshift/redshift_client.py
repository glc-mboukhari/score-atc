import psycopg2
from psycopg2 import sql
import pandas as pd 

class RedshiftExecutor:
    def __init__(self, database, user, password, host, port):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None

    def connect(self):
        self.connection = psycopg2.connect(
            user=self.user,
            host=self.host,
            password=self.password,
            port=self.port,
            database=self.database
        )

    def substitute_parameters(self, sql_text, parameters):
        """
        replace in sql queries the dates parameters (start/end)
        if specified none in functions, no parameters
        """
        if parameters:
            for key, value in parameters.items():
                sql_text = sql_text.replace(f'{{{{{key}}}}}', value)
        return sql_text

    def execute_sql_file(self, file_path, parameters=None):
        """
        open file with sql queries and execute them.
        No need to fetch results since it's only about creating tables
        """
        with open(file_path, 'r') as file:
            sql_commands = file.read()

        sql_commands = self.substitute_parameters(sql_commands, parameters)

        with self.connection.cursor() as cursor:
            cursor.execute(sql.SQL(sql_commands))
            self.connection.commit()

    def fetch_results(self, query, parameters=None):
        """
        execute query and retrieve results
        """
        query = self.substitute_parameters(query, parameters)
        with self.connection.cursor() as cursor:
            cursor.execute(sql.SQL(query))
            results = cursor.fetchall()
            return results

    def execute_query(self, query, parameters=None):
        """
        execute query
        """
        query = self.substitute_parameters(query, parameters)
        with self.connection.cursor() as cursor:
            cursor.execute(sql.SQL(query))
            self.connection.commit()

    def close(self):
        """
        close connection
        """
        if self.connection:
            self.connection.close()
            
    def fetch_data(self, query: str) -> pd.DataFrame:
        """
        return dataframe from results
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(data, columns=columns)
            
if __name__ == "__main__":
    executor = RedshiftExecutor(
        dbname='your_dbname',
        user='your_username',
        password='your_password',
        host='your_redshift_cluster_endpoint'
    )

    executor.connect()

    # Example of executing SQL file with parameters
    parameters = {
        'start_date': '2024-07-30',
        'end_date': '2024-08-02'
    }
    executor.execute_sql_file('queries.sql', parameters)
    
    # Example of fetching results from a query without parameters
    results = executor.fetch_results('SELECT * FROM example_table')
    for row in results:
        print(row)

    # Example of executing a query without parameters
    executor.execute_query('DELETE FROM example_table WHERE id = 1')

    executor.close()
