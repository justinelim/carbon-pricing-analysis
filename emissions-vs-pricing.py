import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Create DynamicFrame(s) from raw table(s)
emissions_pricing_time = glueContext.create_dynamic_frame.from_catalog(
    database="carbon-analysis-db",
    table_name="emissions_pricing_time",
    transformation_ctx="emissions_pricing_time",
)

emissions_reduction = glueContext.create_dynamic_frame.from_catalog(
    database="carbon-analysis-db",
    table_name="emissions_reduction",
    transformation_ctx="emissions_reduction",
)

# Convert to Spark DataFrame
emissions_pricing_time_df = emissions_pricing_time.toDF().select('country', 'year', 'tax', 'ets', 'tax_rate_incl_ex_clcu')
emissions_reduction_df = emissions_reduction.toDF().where(f.col('yoy_rate').isNotNull())

emissions_vs_pricing_df = emissions_reduction_df.join(emissions_pricing_time_df, (emissions_reduction_df.country==emissions_pricing_time_df.country) & (emissions_reduction_df.year==emissions_pricing_time_df.year))

emissions_vs_pricing_df.show()

emissions_vs_pricing_dyF = DynamicFrame.fromDF(emissions_vs_pricing_df, glueContext, "emissions_vs_pricing")

S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=emissions_vs_pricing_dyF,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://carbon-data-analysis/processed-tables/emissions-vs-pricing/",
        "compression": "gzip",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
