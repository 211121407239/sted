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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1711881856443 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-lh/customer/landing/"], "recurse": True},
    transformation_ctx="AmazonS3_node1711881856443",
)

# Script generated for node SQL Query
SqlQuery86 = """
select * from customer_landing
WHERE sharewithresearchasofdate IS NOT NULL;
"""
SQLQuery_node1711880542525 = sparkSqlQuery(
    glueContext,
    query=SqlQuery86,
    mapping={"myDataSource": AmazonS3_node1711881856443},
    transformation_ctx="SQLQuery_node1711880542525",
)

# Script generated for node Amazon S3
AmazonS3_node1711881004226 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1711880542525,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lh/customer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1711881004226",
)

job.commit()
