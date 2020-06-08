from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import split, col, explode, regexp_replace, collect_list
import urllib.request
from datetime import datetime


# Create Spark session
spark = SparkSession.builder \
    .appName("imdb") \
    .master("local[3]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


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

print("Script started at:")
print(datetime.now())
start_time = datetime.now()

print("Processing name_basics")
(
    spark.read.format("com.databricks.spark.csv").option("header", "true")\
        .option("sep", "\t")\
        .option("inferSchema", "true")\
        .option("nullValue", "\\N")\
        .load("/home/student/Downloads/imdb/name.basics.tsv.gz")\
        .withColumn("primary_profession", split(col("primaryProfession"), ",").cast("array<string>"))\
        .withColumn("known_for_titles", split(col("knownForTitles"), ",").cast("array<string>"))\
        .withColumnRenamed("primaryName", "primary_name")\
        .withColumnRenamed("birthYear", "birth_year")\
        .withColumnRenamed("deathYear", "death_year")\
        .drop("primaryProfession", "knownForTitles")
        .repartition(3, "nconst").write.format("parquet").mode("overwrite")
        .save("hdfs://localhost:9000/user/student/imdb_spark_output/name/")
)


print("Processing title_akas")
# Title AKAs
(
    spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("nullValue", "\\N")
        .load("/home/student/Downloads/imdb/title.akas.tsv.gz")
        .withColumnRenamed("titleId", "title_id")
        .withColumnRenamed("isOriginalTitle", "is_original_title")
        .repartition(2, "title_id").write.format("parquet").mode("overwrite")
        .save("hdfs://localhost:9000/user/student/imdb_spark_output/title_akas/")
)

# Title Basics

print("Processing title_basics")
(
    spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("nullValue", "\\N")
        .option("quote", "")
        .load("/home/student/Downloads/imdb/title.basics.tsv.gz")
        .withColumnRenamed("titleType", "title_type")
        .withColumnRenamed("primaryTitle", "primary_title")
        .withColumnRenamed("originalTitle", "original_title")
        .withColumnRenamed("isAdult", "is_adult")
        .withColumnRenamed("startYear", "start_year")
        .withColumnRenamed("endYear", "end_year")
        .withColumnRenamed("runtimeMinutes", "runtime_minutes")
        .withColumn("genre_list", split(col("genres"), ",").cast("array<string>"))
        .drop("genres")
        .withColumnRenamed("genre_list", "genres")
        .repartition(2, "tconst").write.format("parquet").mode("overwrite")
        .save("hdfs://localhost:9000/user/student/imdb_spark_output/title_basics/")
)

print("Processing title_crew")
(
    spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("nullValue", "\\N")
        .option("quote", "")
        .load("/home/student/Downloads/imdb/title.crew.tsv.gz")
        .withColumn("directors_list", split(col("directors"), ",").cast("array<string>"))
        .withColumn("writers_list", split(col("writers"), ",").cast("array<string>"))
        .drop("directors", "writers")
        .withColumnRenamed("directors_list", "directors")
        .withColumnRenamed("writers_list", "writers")
        .repartition(1, "tconst").write.format("parquet").mode("overwrite")
        .save("hdfs://localhost:9000/user/student/imdb_spark_output/title_crew/")
)

print("Processing title_episode")
(
    spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("nullValue", "\\N")
        .option("quote", "")
        .load("/home/student/Downloads/imdb/title.episode.tsv.gz")
        .withColumnRenamed("parentTconst", "parent_tconst")
        .repartition(1, "tconst").write.format("parquet").mode("overwrite")
        .save("hdfs://localhost:9000/user/student/imdb_spark_output/title_episode/")
)

print("Processing title_principals")
(
    spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("nullValue", "\\N")
        .option("quote", "")
        .load("/home/student/Downloads/imdb/title.principals.tsv.gz")
        .withColumn("characters_exploded", explode(split( regexp_replace(col("characters"), r'\[|\]', '')   , '","').cast("array<string>")))
        .withColumn("characters_clean", regexp_replace(col("characters_exploded"), r'\"', ''))
        .groupBy("tconst","ordering", "nconst", "category", "job", "characters")
        .agg(collect_list("characters_clean").alias("characters_array"))
        .drop("characters")
        .withColumnRenamed("characters_exploded", "characters")
        .repartition(3, "tconst").write.format("parquet").mode("overwrite")
        .save("hdfs://localhost:9000/user/student/imdb_spark_output/title_principals/")
)

print("Processing title_ratings")
(
    spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("nullValue", "\\N")
        .option("quote", "")
        .load("/home/student/Downloads/imdb/title.ratings.tsv.gz")
        .withColumnRenamed("averageRating", "average_rating")
        .withColumnRenamed("numVotes", "num_votes")
        .repartition(1, "tconst").write.format("parquet").mode("overwrite")
        .save("hdfs://localhost:9000/user/student/imdb_spark_output/title_ratings/")
)

print("Script Ended at:")
print(datetime.now())
end_time = datetime.now()
diff = (end_time - start_time)

print("Time taken to execute script {diff_mins} minutes ({diff_sec} seconds)".format(diff_mins=(diff.total_seconds()/60), diff_sec=diff.total_seconds()))

