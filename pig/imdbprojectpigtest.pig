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
	2. remove headers
	3. tokenize (split chararray into several tokens) where necessary.
	4. store modified file as tsv in hdfs folder.
	5. store as parquet file in hdfs folder. */
	
name_basics = load 'hdfs://localhost:9000/user/student/imdbproject/name.basics.tsv' using PigStorage('\t') as (nconst:chararray, primaryName:chararray, birthYear:int, deathYear:int, primaryProfession:chararray, knownForTitles:chararray);
ranked_name = rank name_basics;
no_header_name = filter ranked_name by (ranked_name > 1);
ordered_name = order no_header_name by ranked_name;
name_basics = foreach ordered generate nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles;
name_basics = foreach name_basics generate nconst, primaryName, birthYear, deathYear, TOKENIZE(primaryProfession, ',') as primaryProfession, TOKENIZE(knownForTitles, ',') as knownForTitles;
store name_basics into 'hdfs://localhost:9000/user/student/imdbproject/name_basics.tsv' using PigStorage('\t');

title_akas = load 'hdfs://localhost:9000/user/student/imdbproject/title.akas.tsv' using PigStorage('\t') as (titleId:chararray, ordering:int, title:chararray, region:chararray, language:chararray, types:chararray, attributes:chararray, isOriginalTitle:int) 
ranked_akas = rank title_akas;
no_header_akas = filter ranked_akas by (ranked_akas > 1);
ordered_akas = order no_header_akas by ranked_akas;
title_akas = foreach title_akas generate titleId, ordering, title, region, language, types, attributes, isOriginalTitle;
store title_akas into 'hdfs://localhost:9000/user/student/imdbrpoejct/title_akas.tsv' using PigStorage('\t');

title_basics = load 'hdfs://localhost:9000/user/student/imdbproject/title.basics.tsv' using PigStorage('\t') as (tconst:chararray, titleType:chararray, primaryTitle:chararray, originalTitle:chararray, isAdult:int, startYear:int, endYear:int, runtimeMinutes:int, generes:chararray);
ranked_basics = rank title_basics;
no_header_basics = filter ranked_basics by (ranked_basics > 1);
ordered_basics = order no_header_basics by ranked_basics;
title_basics = foreach title_basics generate tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes;
title_basics = foreach title_basics generate tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, TOKENIZE(genres, ',') as genres;
store title_basics into 'hdfs://localhost:9000/user/student/imdbproject/title_basics.tsv' using PigStorage('\t');

title_crew = load 'hdfs://localhost:9000/user/student/imdbproject/title.crew.tsv' using PigStorage('\t') as (tconst:chararray, directors:chararray, writers:chararray);
ranked_crew = rank title_crew;
no_header_crew = filter ranked_crew by (ranked_crew > 1);
ordered_crew = order no_header_crew by ranked_crew;
title_crew = foreach title_crew generate tconst, directors, writers;
title_crew = foreach title_crew generate tconst, TOKENIZE(directors, ',') as directors, TOKENIZE(writers, ',') as writers;
store title_crew into 'hdfs://localhost:9000/user/student/imdbproject/title_crew.tsv' using PigStorage('\t');

title_episode = load 'hdfs://localhost:9000/user/student/imdbproject/title.episode.tsv' using PigStorage('\t') as (tconst:chararray, parentTconst:chararray, seasonNumber:int, episodeNumber:int);
ranked_episode = rank title_episode;
no_header_episode = filter ranked_episode by (ranked_episode > 1);
ordered_episode = order no_header_episode by ranked_episode;
title_episode = foreach title_episode generate tconst, parentTconst, seasonNumber, episodeNumber;
store title_episode into 'hdfs://localhost:9000/user/student/imdbproject/title_episode.tsv' using PigStorage('\t');

title_principals = load 'hdfs://localhost:9000/user/student/imdbproject/title.principals.tsv' using PigStorage('\t') as (tconst:chararray, ordering:int, nconst:chararray, category:chararray, job:chararray, characters:chararray);
ranked_principals = rank title_principals;
no_header_principals = filter ranked_principals by (ranked_principals > 1);
ordered_principals = order no_header_principals by ranked_principals;
title_principals = foreach title_principals generate tconst, ordering, nconst, category, job, characters;
title_principals = foreach title_principals generate tconst, ordering, nconst, category, job, REPLACE(characters, '","', '|') as characters;
title_principals = foreach title_principals generate tconst, ordering, nconst, category, job, TOKENIZE(characters, '|') as characters;
store title_principals into 'hdfs://localhost:9000/user/student/imdbproject/title_principals.tsv' using PigStorage('\t');

title_ratings = load 'hdfs://localhost:9000/user/student/imdbproject/title.ratings.tsv' using PigStorage('\t') as (tconst:chararray, averageRating:double, numVotes:int);
ranked_ratings = rank title_ratings;
no_header_ratings = filter ranked_ratings by (ranked_ratings > 1);
ordered_ratings = order no_header_ratings by ranked_ratings;
title_ratings = foreach title_ratings generate tconst, averageRating, numVotes;
store title_ratings into 'hdfs://localhost:9000/user/student/imdbproject/title_ratings.tsv' using PigStorage('\t');

/* Store as parquet */
store name_basics into 'hdfs://localhost:9000/user/student/imdbproject/name_basics.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_akas into 'hdfs://localhost:9000/user/student/imdbproject/title_akas.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_basics into 'hdfs://localhost:9000/user/student/imdbproject/title_basics.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_crew into 'hdfs://localhost:9000/user/student/imdbproject/title_crew.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_episode into 'hdfs://localhost:9000/user/student/imdbproject/title_episode.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_principals into 'hdfs://localhost:9000/user/student/imdbproject/title_principals.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_ratings into 'hdfs://localhost:9000/user/student/imdbproject/title_ratings.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();



