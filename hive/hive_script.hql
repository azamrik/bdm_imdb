-- Recreate name_basics
drop table if exists name_basics;
drop table if exists name_basics_pq;

create table if not exists name_basics(
    nconst string,
    primaryName string,
    birthYear int, 
    deathYear int, 
    primaryProfession array<string>, 
    knownforTitles array<string>
    ) 
comment 'name basics' 
row format delimited fields terminated by '\t' 
collection items terminated by ',' 
tblproperties(
    'skip.header.line.count'='1'
    'serialization.null.format'='\\N'
);


create table if not exists name_basics_pq(
    nconst string, 
    primaryName string, 
    birthYear int, 
    deathYear int, 
    primaryProfession array<string>, 
    knownforTitles array<string>
    ) 
stored as Parquet;

drop table if exists title_akas;
drop table if exists title_akas_pq;

create table if not exists title_akas(
    titleId string, 
    ordering int, 
    title string, 
    region string, 
    language string, 
    types string, 
    attributes string, 
    isOriginalTitle boolean
    ) 
comment 'title AKAs' 
row format delimited fields terminated by '\t' 
collection items terminated by ',' 
tblproperties(
    'skip.header.line.count'='1',
    'serialization.null.format'='\\N'
);

create table if not exists title_akas_pq(
    titleId string, 
    ordering int, 
    title string, 
    region string, 
    language string, 
    types string, 
    attributes string, 
    isOriginalTitle boolean
    ) 
stored as Parquet;

drop table if exists title_basics;
drop table if exists title_basics_pq;

create table if not exists title_basics(
    tconst string, 
    titleType string, 
    primaryTitle string, 
    originalTitle string, 
    isAdult boolean, 
    startYear int, 
    endYear int, 
    runtimeMinutes int, 
    genres array<string>
    ) 
comment 'title basics' 
row format delimited fields terminated by '\t'
collection items terminated by ',' 
tblproperties(
    'skip.header.line.count'='1',
    'serialization.null.format'='\\N'
);

create table if not exists title_basics_pq(
    tconst string, 
    titleType string,
    primaryTitle string, 
    originalTitle string, 
    isAdult boolean, 
    startYear int, 
    endYear int, 
    runtimeMinutes int, 
    genres array<string>
    )
stored as Parquet;

drop table if exists title_crew;
drop table if exists title_crew_pq;

create table if not exists title_crew(
    tconst string, 
    directors array<string>, 
    writers array<string>
    ) 
comment 'title crew' 
row format delimited fields terminated by '\t' 
collection items terminated by ',' 
tblproperties(
    'skip.header.line.count'='1',
    'serialization.null.format'='\\N'
);


create table if not exists title_crew_pq(
    tconst string, 
    directors array<string>, 
    writers array<string>
    ) 
stored as Parquet;

drop table if exists title_episode;
drop table if exists title_episode_pq;
create table if not exists title_episode(
    tconst string, 
    parentTconst string, 
    seasonNumber int, 
    episodeNumber int
) comment 'title episode' 
row format delimited fields terminated by '\t' 
tblproperties(
    'skip.header.line.count'='1',
    'serialization.null.format'='\\N'
);

create table if not exists title_episode_pq(
    tconst string, 
    parentTconst string, 
    seasonNumber int, 
    episodeNumber int
    )
comment 'title episode'
stored as Parquet;

drop table if exists title_principal;
drop table if exists title_principal_pq;

create table if not exists title_principal(
    tconst string, 
    ordering int, 
    nconst string, 
    category string, 
    job string, 
    characters string
    ) 
comment 'title principal' 
row format delimited fields terminated by '\t' 
tblproperties(
    'skip.header.line.count'='1',
    'serialization.null.format'='\\N'
);


create table if not exists title_principal_pq(
    tconst string,
    ordering int, 
    nconst string, 
    category string, 
    job string, 
    characters array<string>
    ) 
stored as Parquet;


drop table if exists title_ratings;
drop table if exists title_ratings_pq;

create table if not exists title_ratings(
    tconst string, 
    averageRating double, 
    numVotes int
    ) 
comment 'title ratings' 
row format delimited fields terminated by '\t' 
tblproperties(
    'skip.header.line.count'='1',
    'serialization.null.format'='\\N'
);

create table if not exists title_ratings_pq(
    tconst string, 
    averageRating double, 
    numVotes int
    )
comment 'title ratings' 
stored as Parquet;

-- load data from local to hive (every day)
load data local inpath '/home/student/imdb/data/name.basics.tsv.gz' overwrite into table name_basics;
load data local inpath '/home/student/imdb/data/title.akas.tsv.gz' overwrite into table title_akas;
load data local inpath '/home/student/imdb/data/title.basics.tsv.gz' overwrite into table title_basics;
load data local inpath '/home/student/imdb/data/title.crew.tsv.gz' overwrite into table title_crew;
load data local inpath '/home/student/imdb/data/title.episode.tsv.gz' overwrite into table title_episode;
load data local inpath '/home/student/imdb/data/title.principals.tsv.gz' overwrite into table title_principal;
load data local inpath '/home/student/imdb/data/title.ratings.tsv.gz' overwrite into table title_ratings;

set parquet.compression=snappy;

insert overwrite table name_basics_pq
    select nconst,
        primaryName,
        birthYear, 
        deathYear, 
        case when size(primaryProfession ) == 0 then null else primaryProfession end as primaryProfession, 
        case when size(knownforTitles) == 0 then null else knownforTitles end as knownforTitles
    from name_basics;

insert overwrite table title_akas_pq 
    select *
    from title_akas;

insert overwrite table title_basics_pq
    select * 
    from title_basics;

insert overwrite table title_crew_pq 
    select * 
    from title_crew;

insert overwrite table title_episode_pq
    select * 
    from title_episode;

insert overwrite table title_principal_pq
    select tconst, 
        `ordering`, 
        nconst, 
        case when category == '\\N' then null else category end as category, 
        case when job == '\\N' then null else job end as job, 
        split(regexp_replace(case when `characters` == '\\N' then null else `characters` end,'\\[\\"|\\"\\]', ''), '\\",\\"') as `characters`
    from title_principal;

-- insert overwrite table title_principal_pq select tconst, ordering, nconst, category, job, array(regexp_replace(characters,\"\"|\\[|\\]\",\"\")) as characters from title_principal;"
insert overwrite table title_ratings_pq
    select * 
    from title_ratings;
