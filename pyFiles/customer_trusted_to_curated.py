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
AmazonS3_node1711886674247 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1711886674247")

# Script generated for node Amazon S3
AmazonS3_node1711886652699 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/customer/trusted/"]}, transformation_ctx="AmazonS3_node1711886652699")

# Script generated for node SQL Query
SqlQuery1846 = '''
select * from CustomerTrusted a
JOIN AccelerometerLanding b
ON a.email = b.user

'''
SQLQuery_node1711886713783 = sparkSqlQuery(glueContext, query = SqlQuery1846, mapping = {"CustomerTrusted":AmazonS3_node1711886652699, "AccelerometerLanding":AmazonS3_node1711886674247}, transformation_ctx = "SQLQuery_node1711886713783")

# Script generated for node Amazon S3
AmazonS3_node1711886849495 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1711886713783, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lh/customer/curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1711886849495")

job.commit()