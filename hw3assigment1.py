
# creating spark session

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, broadcast, desc


spark = SparkSession.builder.appName("match_medals")\
                            .config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()

spark



# Loading csv data

match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
matches = spark.read.option("header","true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")
medals  = spark.read.option("header","true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header","true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")


#bucketing three csv files 

match_details.select(col("match_id"),col("player_gamertag"),col("player_total_kills"),col("player_total_deaths")).write.bucketBy(16, "match_id").mode("overwrite").saveAsTable("bootcamp.match_details_buck") 


matches.select(col("match_id"),col("is_team_game"),col("playlist_id"),col("completion_date")).write.bucketBy(16, "match_id").mode("overwrite").saveAsTable("bootcamp.matches_buck") 

medals_matches_players.select(col("match_id"),col("medal_id"),col("count")).write.bucketBy(16, "match_id").mode("overwrite").saveAsTable("bootcamp.medals_matches_players_buck")  


#reloading the bucketed data
match_details = spark.table("bootcamp.match_details_buck")
matches = spark.table("bootcamp.matches_buck")
medals_matches_players = spark.table("bootcamp.medals_matches_players_buck")

#aggregating the data by joining all tables
Aggregated_df = match_details\
                .join(matches,"match_id")\
                .join(medals_matches_players,"match_id")\
                .join(medals,"medal_id")\
                .join(maps)
Aggregated_df.cache()
Aggregated_df.show(1)

#1 average most kills per game
most_avg_kills_player = Aggregated_df.groupBy(col("player_gamertag"))\
                        .agg(_sum(col("player_total_kills")).alias("total_kills"),countDistinct(col("match_id")).alias("games_played"))\
                        .withColumn("avg_kills_per_game",col("total_kills")/col("games_played")).orderBy(desc("avg_kills_per_game"))\
                        .limit(1)
most_avg_kills_player.cache()
most_avg_kills_player.show(1,truncate=False)

#most played playlist
most_played_playlist = Aggregated_df.groupBy("playlist_id") \
    .agg(count("*").alias("num_games")) \
    .orderBy(desc("num_games")) \
    .limit(1)

most_played_playlist.show(truncate=False)

#most_played_map

most_played_map = Aggregated_df.groupBy("map_id")\
                            .agg(count("*").alias("num_maps"))\
                            .orderBy(desc("num_maps"))\
                            .limit(1)
most_played_map.show()

#most killing spree medals map
killing_spree_df = Aggregated_df.filter(col("name") == "Killing Spree")

most_killing_spree_map = killing_spree_df.groupBy("map_id") \
    .agg(sum("count").alias("killing_spree_total")) \
    .orderBy(desc("killing_spree_total")) \
    .limit(1)
most_killing_spree_map.show(truncate=False)

#sort with aggregated data
sorted_by_playlist = Aggregated_df.repartition(8).sortWithinPartitions("playlist_id")

sorted_by_playlist.write.mode("overwrite").saveAsTable("bootcamp.events_sorted_playlist")


sorted_by_map = Aggregated_df.repartition(8).sortWithinPartitions("map_id")

sorted_by_map.write.mode("overwrite").saveAsTable("bootcamp.events_sorted_map")

player_sorted = Aggregated_df.repartition(8).sortWithinPartitions("player_gamertag")
player_sorted.write.mode("overwrite").saveAsTable("bootcamp.events_sorted_gamertag")

%%sql

SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_by_plits' 
FROM demo.bootcamp.events_sorted_playlist.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_by_map' 
FROM demo.bootcamp.events_sorted_map.files


%%sql
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files FROM demo.bootcamp.events_sorted_playlist.files;
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files FROM demo.bootcamp.events_sorted_map.files;
                                                                                                                                             
                                                                                                                                             


