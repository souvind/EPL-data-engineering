# Databricks notebook source
# MAGIC %md
# MAGIC # EPL Data Transform Notebook
# MAGIC This notebook reads raw matches, players, and events CSVs, cleans, and writes Parquet outputs.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Paths (update as needed)
matches_path = "dbfs:/FileStore/epl/matches.csv"
players_path = "dbfs:/FileStore/epl/players.csv"
events_path = "dbfs:/FileStore/epl/events.csv"
output_path = "dbfs:/FileStore/epl/processed"

# COMMAND ----------

# DBTITLE 1,Read CSVs
matches = spark.read.option("header","true").csv(matches_path)
players = spark.read.option("header","true").csv(players_path)
events = spark.read.option("header","true").csv(events_path)
display(matches.limit(5))
display(players.limit(5))
display(events.limit(5))

# COMMAND ----------

# DBTITLE 1,Clean & Write Parquet
matches_clean = matches.select(col("match_id").cast("int"), col("season").cast("int"), "home_team", "away_team", "home_score", "away_score")
matches_clean.write.mode("overwrite").parquet(output_path + "/matches")
players.selectExpr("cast(player_id as int) as player_id", "player_name", "team", "position").write.mode("overwrite").parquet(output_path + "/players")
events.selectExpr("cast(match_id as int) as match_id", "cast(player_id as int) as player_id", "minute", "event_type").write.mode("overwrite").parquet(output_path + "/events")

print("Done.")
