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
AmazonS3_node1711881073045 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1711881073045")

# Script generated for node Amazon S3
AmazonS3_node1711881080099 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1711881080099")

# Script generated for node SQL Query
SqlQuery1713 = '''
select * from accelerometer_landing a
JOIN customer_trusted b
ON a.user = b.email

'''
SQLQuery_node1711886390505 = sparkSqlQuery(glueContext, query = SqlQuery1713, mapping = {"accelerometer_landing":AmazonS3_node1711881073045, "customer_trusted":AmazonS3_node1711881080099}, transformation_ctx = "SQLQuery_node1711886390505")

# Script generated for node Amazon S3
AmazonS3_node1711886526406 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1711886390505, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lh/accelerometer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1711886526406")

job.commit()