/* Download datasets daily from imdb website (same as Wei Qin) This is perform outside of pig shell. */ 

wget -q https://datasets.imdbws.com/name.basics.tsv.gz -O /home/student/imdb/raw_downloads/name.basics.tsv.gz
wget -q https://datasets.imdbws.com/title.akas.tsv.gz -O /home/student/imdb/raw_downloads/title.akas.tsv.gz
wget -q https://datasets.imdbws.com/title.basics.tsv.gz -O /home/student/imdb/raw_downloads/title.basics.tsv.gz
wget -q https://datasets.imdbws.com/title.crew.tsv.gz -O /home/student/imdb/raw_downloads/title.crew.tsv.gz
wget -q https://datasets.imdbws.com/title.episode.tsv.gz -O /home/student/imdb/raw_downloads/title.episode.tsv.gz
wget -q https://datasets.imdbws.com/title.principals.tsv.gz -O /home/student/imdb/raw_downloads/title.principals.tsv.gz
wget -q https://datasets.imdbws.com/title.ratings.tsv.gz -O /home/student/imdb/raw_downloads/title.ratings.tsv.gz

/* Unzip files first */

gunzip /home/student/imdb/raw_downloads/name.basics.tsv.gz

/* Create folder in HDFS */

hdfs dfs -mkdir hdfs://localhost:9000/user/student/imdbproject

/* Update daily downloaded datasets to imdbproject folder on HDFS */

hdfs dfs -copyFromLocal /home/student/imdb/raw_downloads/name.basics.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal /home/student/imdb/raw_downloads/title.akas.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal /home/student/imdb/raw_downloads/title.basics.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal /home/student/imdb/raw_downloads/title.crew.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal /home/student/imdb/raw_downloads/title.episode.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal /home/student/imdb/raw_downloads/title.principals.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal /home/student/imdb/raw_downloads/title.ratings.tsv hdfs://localhost:9000/user/student/imdbproject


/* Load into PigStorage as table */
pig -x mapreduce

/* set parquet.compression gzip; */

name_basics = load 'hdfs://localhost:9000/user/student/imdbproject/name.basics.tsv' using PigStorage ('\t') as (nconst:chararray, primaryName:chararray, birthYear:int, deathYear:int, primaryProfession:chararray, knownForTitles:chararray);

name_basics = foreach name_basics generate nconst, primaryName, birthYear, deathYear, TOKENIZE(primaryProfession, ',') as primaryProfession, TOKENIZE(knownForTitles, ',') as knownForTitles;

store name_basics into 'hdfs://localhost:9000/user/student/imdbproject/name_basics.tsv' using PigStorage('\t');

/* Store as parquet */
store name_basics into 'hdfs://localhost:9000/user/student/imdbproject/name_basics.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();


