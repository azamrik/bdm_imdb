#######################################################################################################################
#Download files (can manually download or run the line below in the terminal)
##download IMDB dataset (will download and replace existing file, to be run daily)
echo "Downloading name file"
wget -q https://datasets.imdbws.com/name.basics.tsv.gz -O /home/student/imdb/data/name.basics.tsv.gz
echo "Downloading title aka file"
wget -q https://datasets.imdbws.com/title.akas.tsv.gz -O /home/student/imdb/data/title.akas.tsv.gz
echo "Downloading title basics file"
wget -q https://datasets.imdbws.com/title.basics.tsv.gz -O /home/student/imdb/data/title.basics.tsv.gz
echo "Downloading title crew file"
wget -q https://datasets.imdbws.com/title.crew.tsv.gz -O /home/student/imdb/data/title.crew.tsv.gz
echo "Downloading title episodes file"
wget -q https://datasets.imdbws.com/title.episode.tsv.gz -O /home/student/imdb/data/title.episode.tsv.gz
echo "Downloading title principles file"
wget -q https://datasets.imdbws.com/title.principals.tsv.gz -O /home/student/imdb/data/title.principals.tsv.gz
echo "Downloading title ratings"
wget -q https://datasets.imdbws.com/title.ratings.tsv.gz -O /home/student/imdb/data/title.ratings.tsv.gz


#######################################################################################################################
#Initialize the database and create table
#after starting the terminal
# hive
# create database imdb;
# use imdb;

##create table for each files (only need to be created once)
#each table will need 2 table, 1 to store the tsv another one for parquet format
#name_basics
hive -e "create table if not exists name_basics(nconst string, primaryName string, birthYear int, deathYear int, primaryProfession array<string>, knownforTitles array<string>) comment 'name basics' row format delimited fields terminated by '\t' collection items terminated by ',' tblproperties(\"skip.header.line.count\"=\"1\",\"serialization.null.format\"=\"\");"

hive -e "create table if not exists name_basics_pq(nconst string, primaryName string, birthYear int, deathYear int, primaryProfession array<string>, knownforTitles array<string>) stored as Parquet;"

#title_akas
hive -e "create table if not exists title_akas(titleId string, ordering int, title string, region string, language string, types array<string>, attributes array<string>, isOriginalTitle boolean) comment 'title AKAs' row format delimited fields terminated by '\t' collection items terminated by ',' tblproperties(\"skip.header.line.count\"=\"1\",\"serialization.null.format\"=\"\");"

hive -e "create table if not exists title_akas_pq(titleId string, ordering int, title string, region string, language string, types array<string>, attributes array<string>, isOriginalTitle boolean) stored as Parquet;"

#title_basics
hive -e "create table if not exists title_basics(tconst string, titleType string, primaryTitle string, originalTitle string, isAdult boolean, startYear int, endYear int, runtimeMinutes int, genres string) comment 'title basics' row format delimited fields terminated by '\t' tblproperties(\"skip.header.line.count\"=\"1\",\"serialization.null.format\"=\"\");"

hive -e "create table if not exists title_basics_pq(tconst string, titleType string, primaryTitle string, originalTitle string, isAdult boolean, startYear int, endYear int, runtimeMinutes int, genres string) stored as Parquet;"

#title_crew
hive -e "create table if not exists title_crew(tconst string, directors array<string>, writers array<string>) comment 'title crew' row format delimited fields terminated by '\t' collection items terminated by ',' tblproperties(\"skip.header.line.count\"=\"1\",\"serialization.null.format\"=\"\");"

hive -e "create table if not exists title_crew_pq(tconst string, directors array<string>, writers array<string>) stored as Parquet;"

#title_episode
hive -e "create table if not exists title_episode(tconst string, parentTconst string, seasonNumber int, episodeNumber int) comment 'title episode' row format delimited fields terminated by '\t' tblproperties(\"skip.header.line.count\"=\"1\",\"serialization.null.format\"=\"\");"

hive -e "create table if not exists title_episode_pq(tconst string, parentTconst string, seasonNumber int, episodeNumber int) comment 'title episode'stored as Parquet;"

#title_principals
hive -e "create table if not exists title_principal(tconst string, ordering int, nconst string, category string, job string) comment 'title principal' row format delimited fields terminated by '\t' tblproperties(\"skip.header.line.count\"=\"1\",\"serialization.null.format\"=\"\");"

hive -e "create table if not exists title_principal_pq(tconst string, ordering int, nconst string, category string, job string) stored as Parquet;"

#title_ratings
hive -e "create table if not exists title_ratings(tconst string, averageRating double, numVotes int) comment 'title ratings' row format delimited fields terminated by '\t' tblproperties(\"skip.header.line.count\"=\"1\",\"serialization.null.format\"=\"\");"

hive -e "create table if not exists title_ratings_pq(tconst string, averageRating double, numVotes int) comment 'title ratings' stored as Parquet;"

#convert text format into parquet (if required), this code is not working
#error when reading the table after converting
#alter table table_name set fileformat Parquet;

##############################################################################
##load data from local to hive (every day)
hive -e "load data local inpath '/home/student/imdb/data/name.basics.tsv.gz' overwrite into table name_basics;"
hive -e "load data local inpath '/home/student/imdb/data/title.akas.tsv.gz' overwrite into table title_akas;"
hive -e "load data local inpath '/home/student/imdb/data/title.basics.tsv.gz' overwrite into table title_basics;"
hive -e "load data local inpath '/home/student/imdb/data/title.crew.tsv.gz' overwrite into table title_crew;"
hive -e "load data local inpath '/home/student/imdb/data/title.episode.tsv.gz' overwrite into table title_episode;"
hive -e "load data local inpath '/home/student/imdb/data/title.principals.tsv.gz' overwrite into table title_principal;"
hive -e "load data local inpath '/home/student/imdb/data/title.ratings.tsv.gz' overwrite into table title_ratings;"

##load data from hive tsv table to parquet
hive -e "insert overwrite table name_basics_pq select * from name_basics;"
hive -e "insert overwrite table title_akas_pq select * from title_akas;"
hive -e "insert overwrite table title_basics_pq select * from title_basics;"
hive -e "insert overwrite table title_crew_pq select * from title_crew;"
hive -e "insert overwrite table title_episode_pq select * from title_episode;"
hive -e "insert overwrite table title_principal_pq select * from title_principal;"
hive -e "insert overwrite table title_ratings_pq select * from title_ratings;"

