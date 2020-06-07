#!/bin/sh
# This is a shell script for calling wget and running pig script.
echo "Downloading name file"
wget -q https://datasets.imdbws.com/name.basics.tsv.gz -O /home/student/imdb/raw_downloads/name.basics.tsv.gz
echo "Downloading title aka file"
wget -q https://datasets.imdbws.com/title.akas.tsv.gz -O /home/student/imdb/raw_downloads/title.akas.tsv.gz
echo "Downloading title basics file"
wget -q https://datasets.imdbws.com/title.basics.tsv.gz -O /home/student/imdb/raw_downloads/title.basics.tsv.gz
echo "Downloading title crew file"
wget -q https://datasets.imdbws.com/title.crew.tsv.gz -O /home/student/imdb/raw_downloads/title.crew.tsv.gz
echo "Downloading title episodes file"
wget -q https://datasets.imdbws.com/title.episode.tsv.gz -O /home/student/imdb/raw_downloads/title.episode.tsv.gz
echo "Downloading title principles file"
wget -q https://datasets.imdbws.com/title.principals.tsv.gz -O /home/student/imdb/raw_downloads/title.principals.tsv.gz
echo "Downloading title ratings"
wget -q https://datasets.imdbws.com/title.ratings.tsv.gz -O /home/student/imdb/raw_downloads/title.ratings.tsv.gz

# Unzip files first. -f option to overwrite old stale files

gunzip -f /home/student/imdb/raw_downloads/name.basics.tsv.gz
gunzip -f /home/student/imdb/raw_downloads/title.akas.tsv.gz
gunzip -f /home/student/imdb/raw_downloads/title.basics.tsv.gz
gunzip -f /home/student/imdb/raw_downloads/title.crew.tsv.gz
gunzip -f /home/student/imdb/raw_downloads/title.episode.tsv.gz
gunzip -f /home/student/imdb/raw_downloads/title.principals.tsv.gz
gunzip -f /home/student/imdb/raw_downloads/title.ratings.tsv.gz

# Create folder in HDFS

hdfs dfs -mkdir hdfs://localhost:9000/user/student/imdbproject

# Update daily downloaded datasets to imdbproject folder on HDFS. -f option to overwrite old stale files

hdfs dfs -copyFromLocal -f /home/student/imdb/raw_downloads/name.basics.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal -f /home/student/imdb/raw_downloads/title.akas.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal -f /home/student/imdb/raw_downloads/title.basics.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal -f /home/student/imdb/raw_downloads/title.crew.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal -f /home/student/imdb/raw_downloads/title.episode.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal -f /home/student/imdb/raw_downloads/title.principals.tsv hdfs://localhost:9000/user/student/imdbproject
hdfs dfs -copyFromLocal -f /home/student/imdb/raw_downloads/title.ratings.tsv hdfs://localhost:9000/user/student/imdbproject

# Run pig script
pig -f /home/student/bdm_imdb/pig/imdb_pig_script.pig
