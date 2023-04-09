import argparse
import errno
import json
import logging
import ntpath
import os
import threading
import sys
from os import getenv
from pathlib import Path
from time import localtime
from hashlib import md5
from datetime import datetime, timedelta

import io
import boto3
import magic
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
from hurry.filesize import size, si
from os import getenv
from urllib.request import urlopen

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--bucket_name', '-bn', type=str, help='Name of S3 bucket')
parser.add_argument('--url', type=str, help='Link to download file')
parser.add_argument('--file_name', '-fn', type=str, help='Uploaded file name')
parser.add_argument('--tool', '-t', type=str, help='Choose function')
parser.add_argument('--filepath', '-fp', type=str, help='File path for upload')
parser.add_argument('--multipart_threshold', '-mth', type=int, default=5 * 1024 * 1024 * 1024, help='Multipart threshold in bytes (default: 5GB)')
parser.add_argument('--days', '-d', type=int, help='Number of days when object will be deleted')
parser.add_argument('--memetype', '-mt', type=str, help='Mimetype which is allowed to upload')
parser.add_argument('-del', dest='delete', action='store_true', help='Delete the file')
parser.add_argument('-vers', dest='versioning', action='store_true', help='Check versioning')
parser.add_argument('-verslist', dest='versionlist', action='store_true', help='Version list')
parser.add_argument('-prevers', dest='previous_version', action='store_true', help='Roll back to previous version')
parser.add_argument('-orgobj', dest='organize_objects', action='store_true', help='Put files in folder according extension type')
args = parser.parse_args()

# Load environment variables
load_dotenv()

# Initialize S3 client
s3 = boto3.client('s3')

# Define function to initialize client
def init_client():
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=getenv("aws_access_key_id"),
            aws_secret_access_key=getenv("aws_secret_access_key"),
            aws_session_token=getenv("aws_session_token"),
            region_name=getenv("aws_region_name")
        )
        return client
    except ClientError as e:
        logging.error(e)
    except:
        logging.error("Unexpected error")

def get_s3_client():
    # Create an S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=getenv("aws_access_key_id"),
        aws_secret_access_key=getenv("aws_secret_access_key"),
        region_name=getenv("aws_region_name")
    )
    return s3_client

def list_buckets():
    s3_client = get_s3_client()
    try:
        # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html
        return s3_client.list_buckets()
    except ClientError as e:
        logging.error(e)
        return False

def create_bucket(bucket_name, region=getenv("aws_region_name")):
    s3_client = get_s3_client()
    # Create bucket
    try:
        location = {'LocationConstraint': region}
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html
        response = s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration=location
        )
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False

def delete_bucket(bucket_name):
    s3_client = get_s3_client()
    # Delete bucket
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_bucket.html
        response = s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 204:
        return True
    return False

def bucket_exists(bucket_name):
    s3_client = get_s3_client()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        logging.error(e)
        return False
    return True

def download_file_and_upload_to_s3(bucket_name, url, file_name, keep_local=False):
    s3_client = get_s3_client()
    format = urlopen(url).info()['content-type']
    format = format.split('/')
    formatlist = ["bmp", "jpg", "jpeg", "png", "webp", "mp4"]
    if format[1] in formatlist:
        with urlopen(url) as response:
            content = response.read()
            try:
                s3_client.upload_fileobj(
                    Fileobj=io.BytesIO(content),
                    Bucket=bucket_name,
                    ExtraArgs={'ContentType': 'image/jpg'},
                    Key=file_name
                )
                print("File uploaded successfully!")
            except Exception as e:
                logging.error(e)

        if keep_local:
            with open(file_name, mode='wb') as file:
                file.write(content)
    else:
        print("Uploading of this file type is not allowed!")
    
    # public URL
    return f"https://s3-{getenv('aws_region_name')}.amazonaws.com/{bucket_name}/{file_name}"

def set_object_access_policy(bucket_name, file_name):
    s3_client = get_s3_client()
    try:
        response

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

def create_bucket_policy(s3_client, bucket_name):
    """
    Creates and attaches a bucket policy to grant public read access to the bucket.

    Args:
        s3_client (boto3.client): An initialized S3 client.
        bucket_name (str): The name of the bucket to attach the policy to.
    """
    policy = generate_public_read_policy(bucket_name)
    try:
        s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy)
        logging.info("Bucket policy created successfully for bucket %s.", bucket_name)
    except ClientError as e:
        logging.error("Failed to create bucket policy for bucket %s: %s", bucket_name, e)
        raise

def generate_public_read_policy(bucket_name):
    """
    Generates a bucket policy that grants public read access to the specified bucket.

    Args:
        bucket_name (str): The name of the bucket to generate the policy for.

    Returns:
        str: The JSON-encoded policy document.
    """
    policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Principal': '*',
            'Action': 's3:GetObject',
            'Resource': f'arn:aws:s3:::{bucket_name}/*'
        }]
    }
    return json.dumps(policy)

def read_bucket_policy(s3_client, bucket_name):
    """
    Retrieves and prints the bucket policy.

    Args:
        s3_client (boto3.client): An initialized S3 client.
        bucket_name (str): The name of the bucket to retrieve the policy for.
    """
    try:
        response = s3_client.get_bucket_policy(Bucket=bucket_name)
        policy_str = response['Policy']
        logging.info("Bucket policy for bucket %s: %s", bucket_name, policy_str)
    except ClientError as e:
        logging.error("Failed to retrieve bucket policy for bucket %s: %s", bucket_name, e)
        raise

def upload_file(s3_client, bucket_name, file_path, object_key=None, multipart_threshold=8388608):
    """
    Uploads a file to the specified S3 bucket.

    Args:
        s3_client (boto3.client): An initialized S3 client.
        bucket_name (str): The name of the bucket to upload the file to.
        file_path (str): The local file path of the file to upload.
        object_key (str, optional): The S3 object key to use for the uploaded file. Defaults to None,
            in which case the file name will be used as the object key.
        multipart_threshold (int, optional): The multipart upload threshold, in bytes. Defaults to 8388608.

    Returns:
        str: The S3 object key of the uploaded file.
    """
    if object_key is None:
        object_key = file_path.split('/')[-1]

    try:
        config = boto3.s3.transfer.TransferConfig(multipart_threshold=multipart_threshold)
        with open(file_path, 'rb') as f:
            s3_client.upload_fileobj(f, bucket_name, object_key, Config=config)
        logging.info("File %s uploaded successfully to bucket %s with object key %s", file_path, bucket_name, object_key)
        return object_key
    except ClientError as e:
        logging.error

def upload_file_multipart(s3_client, filepath, bucket_name, file_name, metadata=None):
    MP_THRESHOLD = 1
    MP_CONCURRENCY = 5
    MAX_RETRY_COUNT = 3
    
    log = logging.getLogger('s3_uploader')
    log.setLevel(logging.INFO)
    format = logging.Formatter("%(asctime)s: - %(levelname)s: %(message)s", "%H:%M:%S")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(format)
    log.addHandler(stream_handler)
    
    log.info(f"Uploading [{filepath}] to [{bucket_name}] bucket ...")
    log.info(f"S3 path: [ s3://{bucket_name}/{file_name} ]")
    
    # Check if the file exists
    if not Path(filepath).is_file():
        log.error(f"File [{filepath}] does not exist!")
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filepath)
    
    # Check if the S3 object name is set
    if file_name is None:
        log.error("S3 object must be set!")
        raise ValueError("S3 object must be set!")
    
    # Set the multipart threshold and concurrency
    GB = 1024 ** 3
    mp_threshold = MP_THRESHOLD*GB
    concurrency = MP_CONCURRENCY
    transfer_config = TransferConfig(multipart_threshold=mp_threshold, use_threads=True, max_concurrency=concurrency)
    
    # Attempt to upload the file with retries
    login_attempt = False
    retry = MAX_RETRY_COUNT
    
    while retry > 0:
        try:
            s3_client.upload_file(filepath, bucket_name, file_name, Config=transfer_config, ExtraArgs=metadata)
            sys.stdout.write('\n')
            log.info(f"File [{file_name}] uploaded successfully")
            log.info(f"Object name: [{file_name}]")
            retry = 0
        except ClientError as e:
            log.error("Failed to upload object!")
            log.exception(e)
            if e.response['Error']['Code'] == 'ExpiredToken':
                log.warning('Login token expired')
                retry -= 1
                log.debug(f"retry = {retry}")
                login_attempt = True
                login()
            else:
                log.error("Unhandled error code:")
                log.debug(e.response['Error']['Code'])
                raise
        except boto3.exceptions.S3UploadFailedError as e:
            log.error("Failed to upload object!")
            log.exception(e)
            if 'ExpiredToken' in str(e):
                log.warning('Login token expired')
                log.info("Handling...")
                retry -= 1
                log.debug(f"retry = {retry}")
                login_attempt = True
                login()
            else:
                log.error("Unknown error!")
                raise
    
    if login_attempt:
        raise Exception(f"Tried to login {MAX_RETRY_COUNT} times, but failed to upload!")

def static_website(s3_client, bucket_name, filepath, file_name):
    with open(filepath, 'rb') as f:
        s3_client.upload_fileobj(f, bucket_name, file_name, ExtraArgs={'ContentType': 'text/html'})
    
    website_configuration = {
        'ErrorDocument': {'Key': 'error.html'},
        'IndexDocument': {'Suffix': 'index.html'},
    }
    
    s3_client.put_bucket_website(Bucket=bucket_name, WebsiteConfiguration=website_configuration


if args.tool == 'init_client' or args.tool == 'ic':
    init_client()
    
elif args.tool == 'list_bucket' or args.tool == 'lb':
    buckets = list_buckets(s3_client)
    if buckets:
        for bucket in buckets['Buckets']:
            print(f'  {bucket["Name"]}')
            
elif args.tool == 'create_bucket' or args.tool == 'cb':
    response = s3_client.list_buckets()
    for bucket in response['Buckets']:
        if bucket['Name'] == args.bucket_name:
            print(f'The bucket {args.bucket_name} already exists.')
            break
    else:
        s3_client.create_bucket(Bucket=args.bucket_name)
        print(f'Bucket {args.bucket_name} created.')
        
elif args.tool == 'delete_bucket' or args.tool == 'db':
    response = s3_client.list_buckets()
    for bucket in response['Buckets']:
        if bucket['Name'] == args.bucket_name:
            response = s3_client.delete_bucket(Bucket=args.bucket_name)
            print(f'Bucket {args.bucket_name} deleted.')
            break
    else:
        print(f'Bucket {args.bucket_name} does not exist.')
        
elif args.tool == 'bucket_exists' or args.tool == 'be':
    try:
        bucket_exists(s3_client, args.bucket_name)
        print(f'The bucket {args.bucket_name} exists.')
    except:
        print(f'The bucket {args.bucket_name} does not exist.')
        
elif args.tool == 'set_object_access_policy' or args.tool == 'soap':
    try:
        set_object_access_policy(s3_client, args.bucket_name, args.file_name)
        print("Object access policy added successfully.")
    except:
        print("Object access policy could not be added.")
        
elif args.tool == 'generate_public_read_policy' or args.tool == 'gprp':
    try:
        generate_public_read_policy(args.bucket_name)
        print("Public read policy generated successfully.")
    except:
        print("Public read policy could not be generated.")
        
elif args.tool == 'read_bucket_policy' or args.tool == 'rbp':
    read_bucket_policy(s3_client, args.bucket_name)
    
elif args.tool == 'create_bucket_policy' or args.tool == 'cbp':
    create_bucket_policy(s3_client, args.bucket_name)
    
elif args.tool == "download_file_and_upload_to_s3" or args.tool == 'du':
    download_file_and_upload_to_s3(s3_client, args.bucket_name, args.url, args.file_name, keep_local=False)
    
elif args.tool == "upload" or args.tool == 'u':
    meme = args.filepath.split('.')[-1]
    if args.memetype == meme:
        upload_file(s3_client, args.bucket_name, args.file_name, args.filepath)
    else:
        print(f' Uploading {meme} files is not allowed.')
        
elif args.tool == 'lifecycle' or args.tool == 'lc':
    lifecycle(s3_client , args.bucket_name, args.days)
    
elif args.tool == 'big_file_upload' or args.tool == 'bfu':
    meme = args.filepath.split('.')[-1]
    if args.memetype == meme:
        big_file_upload(s3_client, args.bucket_name, args.file_name, args.filepath, args.multipart_threshold)
    else:
        print(f' Uploading {meme} files is not allowed.')
        
elif args.tool == list_objects or args.tool =='lo
