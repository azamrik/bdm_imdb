from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import split, col
import urllib.request
import time


# Create Spark session
spark = SparkSession.builder \
    .config("spark.executor.memory", "1g")\
    .config("spark.driver.memory", "1g")\
    .appName("imdb") \
    .master("local[4]") \
    .getOrCreate()


base_url = "https://datasets.imdbws.com/"
base_directory = "/home/student/Downloads/imdb/"
files = ["name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
]

for file in files:
    print("Downloading " + file)
    urllib.request.urlretrieve(base_url + file, base_directory + file)

name = spark.read.format("com.databricks.spark.csv").option("header", "true")\
                .option("sep", "\t")\
                .option("inferSchema", "true")\
                .option("nullValue", "\\N")\
                .load("/home/student/Downloads/imdb/name.basics.tsv.gz")\
                .withColumn("primary_profession", split(col("primaryProfession"), ",").cast("array<string>"))\
                .withColumn("knownForTiltes", split(col("knownForTitles"), ",").cast("array<string>"))\
                .withColumnRenamed("primaryName", "primary_name")\
                .withColumnRenamed("birthYear", "birth_year")\
                .withColumnRenamed("deathYear", "death_year")\
                .drop("primaryProfession", "knownForTitles")

name.repartition(5,"nconst").write.format("parquet").mode("overwrite").save("/home/student/Downloads/imdb_spark_output/name/")

