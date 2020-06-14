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


start_time=$(date +%s)
echo "start time: $start_time" 

hive -f /home/student/bdm_imdb/hive/hive_script.hql

end_time=$(date +%s)
echo "end time: $end_time"
diff=$((end_time-start_time))
diff_mins=$(((end_time-start_time)/60))
echo "Time taken to execute script $diff_mins minutes ($diff seconds)"
