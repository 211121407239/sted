import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1711887223308 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/step_trainer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1711887223308")

# Script generated for node Amazon S3
AmazonS3_node1711887244409 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1711887244409")

# Script generated for node SQL Query
SqlQuery1640 = '''
select * from StepTrainerTrusted a
JOIN AccelerometerTrusted b
ON a.sensorReadingTime = b.timeStamp

'''
SQLQuery_node1711887278042 = sparkSqlQuery(glueContext, query = SqlQuery1640, mapping = {"StepTrainerTrusted":AmazonS3_node1711887244409, "AccelerometerTrusted":AmazonS3_node1711887223308}, transformation_ctx = "SQLQuery_node1711887278042")

# Script generated for node Amazon S3
AmazonS3_node1711887340086 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1711887278042, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lh/ML_curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1711887340086")

job.commit()