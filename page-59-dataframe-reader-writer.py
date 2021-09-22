# Import SparkSession
from pyspark.sql import SparkSession
# In Python, define a schema
from pyspark.sql.types import *
# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition',
                                      StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch',
                                      IntegerType(), True),
                          StructField('FirePreventionDistrict',
                                      StringType(), True),
                          StructField('SupervisorDistrict',
                                      StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "gs://wf-ae-hive-staging-prod/test201/LearningSparkV2/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
spark = (SparkSession.builder.appName("Example-Page-59").getOrCreate())
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# Use the DataFrameWriter interface to write to parquet format
parquet_path = "gs://wf-ae-hive-staging-prod/test201/sf_fire_incidents/"
fire_df.write.format("parquet").save(parquet_path)

# In Python
few_fire_df = (fire_df.select("IncidentNumber", "AvailableDtTm", "CallType").where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)