from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, MapType, ArrayType, DateType, \
    TimestampType, FloatType, DoubleType
from pyspark.sql.functions import from_json, col, explode, lower, concat, lit, collect_list, unix_timestamp, udf, avg, round
from pyspark.sql.dataframe import DataFrame
from src.utils.config_parser import default_config, parse_config
from src.streamer.preprocessor import tokenize_dataframe, remove_stopwords

db_config = parse_config("database")

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
    StructField("pub_date", TimestampType(), True),
    StructField("section_name", StringType(), True),
    StructField("subsection_name", StringType(), True),
    StructField("type_of_material", StringType(), True),
])

# Create a SparkSession
spark = SparkSession.builder \
    .appName("NYT Streamer") \
    .config("spark.mongodb.output.uri", f"{db_config['MONGO']['URI']}/{db_config['MONGO']['DB_NAME']}.{db_config['MONGO']['DB_COLLECTION']}") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", default_config["KAFKA"]["BOOTSTRAP_SERVERS"]) \
    .option("subscribe", default_config["KAFKA"]["ARCHIVE_TOPIC"]) \
    .option("startingOffsets", "latest") \
    .load()

parsed_data_df = df.selectExpr("CAST(value AS STRING)")
news_df = parsed_data_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

target_words = ["russia", "putin", "nato", "war", "ukraine"]
relativity_threshold = 0.0


def udf_typed(return_type=StringType()):
    def _typed_udf_wrapper(func):
        return udf(func, return_type)

    return _typed_udf_wrapper


@udf_typed(DoubleType())
def relativity_strength_udf(text_words):
    strength = 0
    for word in target_words:
        if word in text_words:
            strength += 1

    return strength / len(target_words)


def filter_news():
    concat_df = news_df.select("_id", "abstract", "lead_paragraph", col("headline.main").alias("head_main"), explode("keywords").alias("keywords"), "pub_date") \
        .select("_id", "abstract", "lead_paragraph", "head_main", col("keywords.value").alias("keyw_value"), "pub_date") \
        .withColumn("concat_cols", lower(concat(col('abstract'), lit(" "), col('lead_paragraph'), lit(" "), col('head_main'), lit(" "), col('keyw_value'))))

    # Preprocess the concatenated column for text analysis
    tokenized_df = tokenize_dataframe(concat_df, "concat_cols", "tokenized")
    cleaned_df = remove_stopwords(tokenized_df, "tokenized", "cleaned")

    # Find the relativity of the news
    relativity_df = cleaned_df.withColumn("relativity", relativity_strength_udf(col("cleaned")))

    grouped_news = relativity_df \
        .withColumn('timestamp', unix_timestamp(col('pub_date'), "MM/dd/yyyy hh:mm:ss aa").cast(TimestampType())) \
        .withWatermark("timestamp", "1 minutes") \
        .groupBy("_id", "abstract", "lead_paragraph", "head_main", "pub_date", "timestamp") \
        .agg(collect_list("keyw_value").alias("keywords"), round(avg('relativity'), 2).alias('relativity'))

    # Filter the news based on relativity strength given the threshold
    filtered_df = grouped_news.filter(grouped_news.relativity >= relativity_threshold)

    return filtered_df


def store_dataframe(dataframe: DataFrame, batch_id: int):
    dataframe.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", f"{db_config['MONGO']['URI']}") \
        .option("database", db_config["MONGO"]["DB_NAME"]) \
        .option("collection", db_config["MONGO"]["DB_COLLECTION"]) \
        .save()


def run_spark_streamer():
    final_df = filter_news()
    final_df.printSchema()

    final_df.writeStream \
        .foreachBatch(store_dataframe) \
        .start() \
        .awaitTermination()

    spark.stop()
