from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, MapType, ArrayType, DataType
from pyspark.sql.functions import from_json, col, explode
from src.utils.config_parser import default_config

schema = StructType([
    StructField("abstract", StringType(), True),
    StructField("web_url", StringType(), True),
    StructField("lead_paragraph", StringType(), True),
    StructField("source", StringType(), True),
    StructField(
        "headline",
        MapType(StringType(), StringType(), True),
        True
    ),
    StructField(
        "keywords",
        ArrayType(
            StructType([
                StructField("name", StringType(), True),
                StructField("value", StringType(), True),
                StructField("rank", IntegerType(), True),
                StructField("major", StringType(), True),
            ])
        ),
        True
    ),
    StructField("pub_date", DataType(), True),
    StructField("section_name", StringType(), True),
    StructField("subsection_name", StringType(), True),
    StructField("type_of_material", StringType(), True),
])
schema = ArrayType(schema, False)
# Create a SparkSession
spark = SparkSession.builder.appName("NYT Streamer").getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", default_config["KAFKA"]["BOOTSTRAP_SERVERS"]) \
    .option("subscribe", default_config["KAFKA"]["ARCHIVE_TOPIC"]) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_data_df = df.selectExpr("CAST(value AS STRING)")
converted_df = parsed_data_df.select(explode(from_json(col("value"), schema).alias("data"))).select("col.*")
converted_df.printSchema()
converted_df.writeStream \
      .format("console") \
      .option("numRows", 20) \
      .outputMode("append") \
      .start() \
      .awaitTermination()

spark.stop()
