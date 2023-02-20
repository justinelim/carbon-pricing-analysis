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

# Convert to Spark DataFrame
carbon_pricing_df = carbon_pricing.toDF().select('jurisdiction', 'year', 'tax', 'ets', 'tax_rate_incl_ex_clcu')
carbon_emissions_df = carbon_emissions.toDF().withColumnRenamed('annual co₂ emissions', 'annual_co2_emissions')

# Tranformations

carbon_pricing_df = carbon_pricing_df.withColumn('net_tax_rate', f.when(f.col('tax_rate_incl_ex_clcu')=='NA', 'test').otherwise(f.col('tax_rate_incl_ex_clcu')))
carbon_pricing_df = carbon_pricing_df.withColumn('net_tax_rate', f.col('tax_rate_incl_ex_clcu').cast('float'))
carbon_pricing_df = carbon_pricing_df.groupBy('jurisdiction', 'year', 'tax', 'ets').agg(f.avg('net_tax_rate').alias('avg_net_tax_rate'))
# carbon_pricing_df = carbon_pricing_df.groupBy('jurisdiction', 'year', 'avg_net_tax_rate').agg(f.max('tax').alias('tax_yn'), f.max('ets').alias('ets_yn'))
carbon_pricing_df = carbon_pricing_df.groupBy('jurisdiction', 'year').agg(f.max('avg_net_tax_rate').alias('avg_net_tax_rate_clean'), f.max('tax').alias('tax_yn'), f.max('ets').alias('ets_yn'))
# carbon_pricing_df = carbon_pricing_df.na.drop(subset=['avg_net_tax_rate'])

# columns_to_partition_by = ['jurisdiction', 'year']
# window_tax = Window.partitionBy([f.col(x) for x in columns_to_partition_by]).orderBy(f.col('tax_rate_incl_ex_clcu').desc())
# carbon_pricing_df = carbon_pricing_df.withColumn('row', f.row_number().over(window_tax)) \
#   .filter(f.col('row') == 1).drop('row')

emissions_pricing_time_df = carbon_emissions_df.alias('a').join(carbon_pricing_df.alias('b'), (f.lower(carbon_emissions_df.entity) == f.lower(carbon_pricing_df.jurisdiction)) & (carbon_emissions_df.year == carbon_pricing_df.year), 'inner') \
    .select('a.entity', 'code', 'a.year', 'annual_co2_emissions', 'tax_yn', 'ets_yn', 'avg_net_tax_rate_clean')
emissions_pricing_time_df = emissions_pricing_time_df.withColumn('date', f.to_date(f.col('year').cast('string'), 'yyyy'))

# print('EMISSIONS_PRICING_TIME_DF:')
print('BEFORE EMISSIONS_PRICING_TIME_DF:')
emissions_pricing_time_df.where(f.col('entity')=='Finland').orderBy(f.col('date').desc()).show()

columns_to_drop = ('jurisdiction', 'year')
emissions_pricing_time_df = emissions_pricing_time_df.drop(*columns_to_drop).withColumnRenamed('entity', 'country')


# emissions_pricing_time_df.show()

print('DATA TYPES: FOR LOOP')
for col in emissions_pricing_time_df.dtypes:
    print(col[0], col[1])

print('DATA TYPES: PRINTSCHEMA')
emissions_pricing_time_df.printSchema()

print('AFTER EMISSIONS_PRICING_TIME_DF:')
emissions_pricing_time_df.where(f.col('country')=='Finland').orderBy(f.col('date').desc()).show(truncate=False)

# Write to S3
emissions_pricing_time_dyF = DynamicFrame.fromDF(emissions_pricing_time_df, glueContext, "emissions_pricing_time")
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=emissions_pricing_time_dyF,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://carbon-data-analysis/processed-tables/emissions-pricing-time/",
        "compression": "gzip",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
