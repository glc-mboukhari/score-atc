from scripts.redshift.redshift_client import RedshiftExecutor
from process.process_data import ProcessDataForATC
from scripts.s3.s3_client import S3Loader

def main():
    '''
    # Redshift connection details
    dbname = 'your_dbname'
    user = 'your_username'
    password = 'your_password'
    host = 'your_redshift_cluster_endpoint'
    
    # S3 details
    bucket_name = 'your_s3_bucket'
    s3_key = 'path/to/your/output_file.csv'

    # SQL query to extract data
    query = 'SELECT * FROM your_table'

    # Step 1: Extract data from Redshift
    extractor = RedshiftExecutor(dbname, user, password, host)
    extractor.connect()
    data = extractor.fetch_data(query)
    extractor.close()
    """
    '''
    # Step 2: Preprocess the data
    preprocessor = ProcessDataForATC()
    processed_data = preprocessor.preprocess(data)

    # Step 3: Load the processed data to S3
    loader = LoadDataS3(bucket_name)
    loader.upload_dataframe_as_csv(processed_data, s3_key)
    """
if __name__ == "__main__":
    main()
