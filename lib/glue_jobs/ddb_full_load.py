import multiprocessing
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    [
        "PRODUCER_DYNAMODB_NAME",
        "PRODUCER_ACCOUNT_ID",
        "PRODUCER_DYNAMODB_READ_ROLE_NAME",
        "PRODUCER_REGION",
        "S3_BUCKET_NAME",
        "S3_PREFIX",
        "WORKER_TYPE",
        "NUM_WORKERS",
    ],
)


producer_account_id = args["PRODUCER_ACCOUNT_ID"]
producer_ddb_read_role_name = args["PRODUCER_DYNAMODB_READ_ROLE_NAME"]
producer_ddb_name = args["PRODUCER_DYNAMODB_NAME"]
producer_region = args["PRODUCER_REGION"]
s3_bucket_name = args["S3_BUCKET_NAME"]
s3_fixed_prefix = args["S3_PREFIX"]
worker_type = args["WORKER_TYPE"]
num_workers = args["NUM_WORKERS"]

producer_ddb_read_role_arn = (
    f"arn:aws:iam::{producer_account_id}:role/{producer_ddb_read_role_name}"
)

if worker_type == "G.2X":
    ddb_split = 16 * (int(num_workers) - 1)

elif worker_type == "G.1X":
    ddb_split = 8 * (int(num_workers) - 1)
else:
    num_executers = (int(num_workers) - 1) * 2 - 1
    ddb_split = 4 * num_executers

print(str(ddb_split))


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
glue_context = GlueContext(SparkContext.getOrCreate())
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

dyf = glue_context.create_dynamic_frame_from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.region": producer_region,
        "dynamodb.splits": str(ddb_split),
        "dynamodb.throughput.read.percent": "1.2",
        "dynamodb.input.tableName": producer_ddb_name,
        "dynamodb.sts.roleArn": producer_ddb_read_role_arn,
    },
)
dyf.show()
num_output_partitions = multiprocessing.cpu_count()

df = dyf.toDF().repartition(num_output_partitions)
dyf = DynamicFrame.fromDF(df, glue_context, "dyf")

# Writing the dynamic frame to S3 in Parquet format
s3_path = f"s3://{s3_bucket_name}/{s3_fixed_prefix}/full_load"
glue_context.purge_s3_path(s3_path, {"retentionPeriod": 0})
glue_context.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    connection_options={
        "path": s3_path,
    },
    format="parquet",
)

job.commit()
