import sys,os, time
sys.path.extend(['/home/ubuntu/boto'])

import boto
import boto.s3.key
import boto.s3.bucket

program_name = sys.argv[0]
filename_to_upload = sys.argv[1]
destination_name = sys.argv[2]

file_handle = open(filename_to_upload)

x_aws_access_key_id = os.environ.get("x_aws_access_key_id")
x_aws_secret_access_key = os.environ.get("x_aws_secret_access_key")
bucket = os.environ.get("bucket")

print program_name,"started"
t0 = time.time()

conn = boto.connect_s3(
    aws_access_key_id=x_aws_access_key_id,
    aws_secret_access_key = x_aws_secret_access_key,
    )

print time.time()-t0, "connected"
bucket = boto.s3.bucket.Bucket(conn, name=bucket)
print time.time()-t0, "bucket found"
key = boto.s3.key.Key(bucket, name=destination_name)
key.set_contents_from_file(file_handle)
print time.time()-t0, "upload complete"