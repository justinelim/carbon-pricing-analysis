import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(
    glueContext, query, mapping, transformation_ctx
) -> DynamicFrame:
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

# Create DynamicFrame(s) from raw table(s)
carbon_emissions = glueContext.create_dynamic_frame.from_catalog(
    database="carbon-analysis-db",
    table_name="carbon_emissions",
    transformation_ctx="carbon_emissions",
)

carbon_pricing = glueContext.create_dynamic_frame.from_catalog(
    database="carbon-analysis-db",
    table_name="carbon_pricing",
    transformation_ctx="carbon_pricing",
)

# Data cleaning: Rename column with spaces & strange character
carbon_emissions_df = carbon_emissions.toDF().withColumnRenamed('annual co₂ emissions', 'annual_co2_emissions')
carbon_emissions = DynamicFrame.fromDF(carbon_emissions_df, glueContext, "carbon_emissions_clean")

main_sql_query = f"""
    WITH yearly_emissions AS (
        SELECT
            entity AS country,
            year,
            annual_co2_emissions AS curr_year_emission
        FROM carbon_emissions  
    ),
    yearly_variance AS (
        SELECT
            *,
            LAG(curr_year_emission, 1) OVER (PARTITION BY country ORDER BY year) AS prev_year_emission
        FROM yearly_emissions
    )
    SELECT
        year,
        country,
        curr_year_emission,
        prev_year_emission,
        ROUND(100*(curr_year_emission - prev_year_emission)/prev_year_emission, 2) AS yoy_rate
    FROM yearly_variance
"""

etl_sql_node = sparkSqlQuery(
        glueContext,
        query=main_sql_query,
        mapping={
            "carbon_emissions": carbon_emissions,
        },
        transformation_ctx="etl_sql_node",
    )
    
etl_sql_node.toDF().show()

S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=etl_sql_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://carbon-data-analysis/processed-tables/emissions-reduction/",
        "compression": "gzip",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
