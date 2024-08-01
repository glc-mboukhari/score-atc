import boto3
from io import StringIO

class S3Loader:
    def __init__(self, bucket_name, profile_name='default'):
        session = boto3.Session(profile_name=profile_name)
        self.s3 = session.client('s3')
        self.bucket_name = bucket_name

    def upload_file(self, file_path, s3_key):
        """Uploads a file to the specified S3 bucket and key."""
        self.s3.upload_file(file_path, self.bucket_name, s3_key)

    def upload_dataframe_as_csv(self, data, s3_key):
        """Uploads a pandas DataFrame as a CSV to the specified S3 bucket and key."""
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        self.s3.put_object(Bucket=self.bucket_name, Key=s3_key, Body=csv_buffer.getvalue())

    def list_buckets(self):
        """Lists all buckets in the S3 account."""
        response = self.s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        return buckets

if __name__ == "__main__":
    s3 = S3Loader('test_atc', profile_name='development_dev')
    print(s3.list_buckets())