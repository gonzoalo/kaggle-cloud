import logging
import boto3
from botocore.exceptions import ClientError

def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    default region is us-east-1

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return True if bucket created, else False
    """

    # Create bucket 

    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name = region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_dataset(folder_name, bucket_name, folder_object_name=None):
    """Upload the dataset to the S3 bucket 

    default folder name dataset
    :param folder_name: folder within the dataset
    :param bucket_name: Bucket where the dataset will be upload
    :poram folder_object_name: S3 folder name, if not specified then 'kaggle-dataset' is used
    return True if dataset uploaded, else False
    """

    # if folder_object_name was not specified, use folder

    if folder_object_name is None:
        folder_name = 'kaggle '+ folder_name

    # upload folder
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(folder_name, bucket_name, folder_object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def create_directory(directory_name, bucket_name):
    """Create a directory in the S3 bucket

    :param directory_name: name of the directory 
    :param bucket_name: Bcuket where the directory will be created
    return True if directory created, else False
    """

    s3 = boto3.client('s3')
    
    try:
        response = s3.put_object(Bucket=bucket_name, Key =(directory_name+'/'))
    except ClientError as e:
        logging.error(e)
        return False
    return True
