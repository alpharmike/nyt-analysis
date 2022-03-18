from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, MapType, ArrayType, DateType, \
    TimestampType
from pyspark.sql.functions import from_json, col, explode, lower, concat, lit, collect_list, unix_timestamp
from src.utils.config_parser import default_config

schema = StructType([
    StructField("_id", StringType(), True),
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
    StructField("pub_date", DateType(), True),
    StructField("section_name", StringType(), True),
    StructField("subsection_name", StringType(), True),
    StructField("type_of_material", StringType(), True),
])

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
news_df = parsed_data_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

concat_df = news_df.select("_id", "abstract", "lead_paragraph", col("headline.main").alias("head_main"), explode("keywords").alias("keywords"), "pub_date") \
    .select("_id", "abstract", "lead_paragraph", "head_main", col("keywords.value").alias("keyw_value"), "pub_date") \
    .withColumn("concat_cols", concat(col('abstract'), lit(" "), col('lead_paragraph'), lit(" "), col('head_main'), lit(" "), col('keyw_value'))) \

filtered_df = concat_df.filter(
    (lower(concat_df.concat_cols).contains("crimson")) | (lower(concat_df.concat_cols).contains("russia")) \
    | (lower(concat_df.concat_cols).contains("nato")) | (lower(concat_df.concat_cols).contains("war"))
)

grouped_news = filtered_df.groupBy("_id", "abstract", "lead_paragraph", "head_main", "pub_date").agg(collect_list("keyw_value").alias("keywords"))

grouped_news.printSchema()
grouped_news.writeStream \
      .format("console") \
      .option("numRows", 20) \
      .outputMode("complete") \
      .start() \
      .awaitTermination()

spark.stop()
