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


/*	1. load tsv file using PigStorage with tab delimiter.
	2. tokenize (split chararray into several tokens) where necessary.
	3. store modified file as tsv in hdfs folder.
	4. store as parquet file in hdfs folder. */
	
name_basics = load 'hdfs://localhost:9000/user/student/imdbproject/name.basics.tsv' using PigStorage('\t') as (nconst:chararray, primaryName:chararray, birthYear:int, deathYear:int, primaryProfession:chararray, knownForTitles:chararray);
name_basics = foreach name_basics generate nconst, primaryName, birthYear, deathYear, TOKENIZE(primaryProfession, ',') as primaryProfession, TOKENIZE(knownForTitles, ',') as knownForTitles;
store name_basics into 'hdfs://localhost:9000/user/student/imdbproject/name_basics.tsv' using PigStorage('\t');

title_akas = load 'hdfs://localhost:9000/user/student/imdbproject/title.akas.tsv' using PigStorage('\t') as (titleId:chararray, ordering:int, title:chararray, region:chararray, language:chararray, types:chararray, attributes:chararray, isOriginalTitle:int) 
store title_akas into 'hdfs://localhost:9000/user/student/imdbrpoejct/title_akas.tsv' using PigStorage('\t');

title_basics = load 'hdfs://localhost:9000/user/student/imdbproject/title.basics.tsv' using PigStorage('\t') as (tconst:chararray, titleType:chararray, primaryTitle:chararray, originalTitle:chararray, isAdult:int, startYear:int, endYear:int, runtimeMinutes:int, generes:chararray);
title_basics = foreach title_basics generate tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, TOKENIZE(genres, ',') as genres;
store title_basics into 'hdfs://localhost:9000/user/student/imdbproject/title_basics.tsv' using PigStorage('\t');

title_crew = load 'hdfs://localhost:9000/user/student/imdbproject/title.crew.tsv' using PigStorage('\t') as (tconst:chararray, directors:chararray, writers:chararray);
title_crew = foreach title_crew generate tconst, TOKENIZE(directors, ',') as directors, TOKENIZE(writers, ',') as writers;
store title_crew into 'hdfs://localhost:9000/user/student/imdbproject/title_crew.tsv' using PigStorage('\t');

title_episode = load 'hdfs://localhost:9000/user/student/imdbproject/title.episode.tsv' using PigStorage('\t') as (tconst:chararray, parentTconst:chararray, seasonNumber:int, episodeNumber:int);
store title_episode into 'hdfs://localhost:9000/user/student/imdbproject/title_episode.tsv' using PigStorage('\t');

title_principals = load 'hdfs://localhost:9000/user/student/imdbproject/title.principals.tsv' using PigStorage('\t') as (tconst:chararray, ordering:int, nconst:chararray, category:chararray, job:chararray, characters:chararray);
title_principals = foreach title_principals generate tconst, ordering, nconst, category, job, TOKENIZE(characters, ',') as characters;
store title_principals into 'hdfs://localhost:9000/user/student/imdbproject/title_principals.tsv' using PigStorage('\t');

title_ratings = load 'hdfs://localhost:9000/user/student/imdbproject/title.ratings.tsv' using PigStorage('\t') as (tconst:chararray, averageRating:double, numVotes:int);
store title_ratings into 'hdfs://localhost:9000/user/student/imdbproject/title_ratings.tsv' using PigStorage('\t');

/* Store as parquet */
store name_basics into 'hdfs://localhost:9000/user/student/imdbproject/name_basics.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_akas into 'hdfs://localhost:9000/user/student/imdbproject/title_akas.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_basics into 'hdfs://localhost:9000/user/student/imdbproject/title_basics.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_crew into 'hdfs://localhost:9000/user/student/imdbproject/title_crew.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_episode into 'hdfs://localhost:9000/user/student/imdbproject/title_episode.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_principals into 'hdfs://localhost:9000/user/student/imdbproject/title_principals.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_ratings into 'hdfs://localhost:9000/user/student/imdbproject/title_ratings.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();



