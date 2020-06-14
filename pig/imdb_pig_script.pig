/*	1. load tsv file using PigStorage with tab delimiter.
	2. remove headers
	3. tokenize (split chararray into several tokens) where necessary.
	4. store as parquet file in hdfs folder. */
	
name_basics = load 'hdfs://localhost:9000/user/student/imdbproject/name.basics.tsv' using PigStorage('\t') as (nconst:chararray, primaryName:chararray, birthYear:int, deathYear:int, primaryProfession:chararray, knownForTitles:chararray);
name_basics = filter name_basics by (nconst != 'nconst');
name_basics = foreach name_basics generate 
	nconst, 
	primaryName, 
	birthYear,
	deathYear,
	(case primaryProfession when '\\N' then null else primaryProfession end) as primaryProfession, 
	(case knownForTitles when '\\N' then null else knownForTitles end) as knownForTitles;
name_basics = foreach name_basics generate
	nconst, 
	primaryName, 
	birthYear,
	deathYear,
	TOKENIZE(primaryProfession, ',') as primaryProfession,
	TOKENIZE(knownForTitles, ',') as knownForTitles;



title_akas = load 'hdfs://localhost:9000/user/student/imdbproject/title.akas.tsv' using PigStorage('\t') as (titleId:chararray, ordering:int, title:chararray, region:chararray, language:chararray, types:chararray, attributes:chararray, isOriginalTitle:int); 
title_akas = filter title_akas by (titleId != 'titleId');
title_akas = foreach title_akas generate 
	titleId, 
	ordering, 
	title, 
	(case region when '\\N' then null else region end) as region,
	(case language when '\\N' then null else language end) as language,
	(case types when '\\N' then null else types end) as types,
	(case attributes when '\\N' then null else attributes end) as attributes,
	isOriginalTitle;

title_basics = load 'hdfs://localhost:9000/user/student/imdbproject/title.basics.tsv' using PigStorage('\t') as (tconst:chararray, titleType:chararray, primaryTitle:chararray, originalTitle:chararray, isAdult:int, startYear:int, endYear:int, runtimeMinutes:int, genres:chararray);
title_basics = filter title_basics by (tconst != 'tconst');
title_basics = foreach title_basics generate 
	tconst, 
	titleType, 
	primaryTitle, 
	originalTitle, 
	isAdult, 
	startYear, 
	endYear, 
	runtimeMinutes, 
	(case genres when '\\N' then null else genres end) as genres;
title_basics = foreach title_basics generate 
	tconst, 
	titleType, 
	primaryTitle, 
	originalTitle, 
	isAdult, 
	startYear, 
	endYear, 
	runtimeMinutes, 
	TOKENIZE(genres, ',') as genres;

title_crew = load 'hdfs://localhost:9000/user/student/imdbproject/title.crew.tsv' using PigStorage('\t') as (tconst:chararray, directors:chararray, writers:chararray);
title_crew = filter title_crew by (tconst != 'tconst');
title_crew = foreach title_crew generate
	tconst,
	(case directors when '\\N' then null else directors end) as directors,
	(case writers when '\\N' then null else writers end) as writers;
title_crew = foreach title_crew generate 
	tconst, 
	TOKENIZE(directors, ',') as directors, 
	TOKENIZE(writers, ',') as writers;

title_episode = load 'hdfs://localhost:9000/user/student/imdbproject/title.episode.tsv' using PigStorage('\t') as (tconst:chararray, parentTconst:chararray, seasonNumber:int, episodeNumber:int);
title_episode = filter title_episode by (tconst != 'tconst');


title_principals = load 'hdfs://localhost:9000/user/student/imdbproject/title.principals.tsv' using PigStorage('\t') as (tconst:chararray, ordering:int, nconst:chararray, category:chararray, job:chararray, characters:chararray);
title_principals = filter title_principals by (tconst != 'tconst');
title_principals = foreach title_principals generate
	tconst,
	ordering,
	nconst,
	(case category when '\\N' then null else category end) as category,
	(case job when '\\N' then null else job end) as job,
	(case characters when '\\N' then null else characters end) as characters;
title_principals = foreach title_principals generate
	tconst, 
	ordering, 
	nconst, 
	category, 
	job, 
	TOKENIZE(REPLACE(REPLACE(characters, '","', '|'), '\\["|"\\]', '') , '|') as characters;

title_ratings = load 'hdfs://localhost:9000/user/student/imdbproject/title.ratings.tsv' using PigStorage('\t') as (tconst:chararray, averageRating:double, numVotes:int);
title_ratings = filter title_ratings by (tconst != 'tconst');
title_ratings = foreach title_ratings generate tconst, averageRating, numVotes;

/* Remove old files */
rmf hdfs://localhost:9000/user/student/imdbproject/name_basics.snappy.parquet
rmf hdfs://localhost:9000/user/student/imdbproject/title_akas.snappy.parquet
rmf hdfs://localhost:9000/user/student/imdbproject/title_basics.snappy.parquet
rmf hdfs://localhost:9000/user/student/imdbproject/title_crew.snappy.parquet
rmf hdfs://localhost:9000/user/student/imdbproject/title_episode.snappy.parquet
rmf hdfs://localhost:9000/user/student/imdbproject/title_principals.snappy.parquet 
rmf hdfs://localhost:9000/user/student/imdbproject/title_ratings.snappy.parquet

/* Store as parquet */
store name_basics into 'hdfs://localhost:9000/user/student/imdbproject/name_basics.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_akas into 'hdfs://localhost:9000/user/student/imdbproject/title_akas.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_basics into 'hdfs://localhost:9000/user/student/imdbproject/title_basics.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_crew into 'hdfs://localhost:9000/user/student/imdbproject/title_crew.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_episode into 'hdfs://localhost:9000/user/student/imdbproject/title_episode.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_principals into 'hdfs://localhost:9000/user/student/imdbproject/title_principals.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();
store title_ratings into 'hdfs://localhost:9000/user/student/imdbproject/title_ratings.snappy.parquet' using org.apache.parquet.pig.ParquetStorer();



