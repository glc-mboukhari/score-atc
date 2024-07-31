import boto3

class S3Loader:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name

    def upload_file(self, file_path, s3_key):
        self.s3.upload_file(file_path, self.bucket_name, s3_key)

    def upload_dataframe_as_csv(self, data, s3_key):
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        self.s3.put_object(Bucket=self.bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

