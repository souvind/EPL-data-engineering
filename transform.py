# transform.py
# PySpark job to process EPL matches, players, and events CSVs into Parquet and compute sample aggregates.
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count

def main(matches_path, players_path, events_path, output_path):
    spark = SparkSession.builder.appName("epl-transform").getOrCreate()

    matches = spark.read.option("header", "true").csv(matches_path)
    players = spark.read.option("header", "true").csv(players_path)
    events = spark.read.option("header", "true").csv(events_path)

    # Basic cleaning / type casting for matches
    matches_clean = matches.select(
        col("match_id").cast("int"),
        col("season").cast("int"),
        col("date"),
        col("home_team"),
        col("away_team"),
        col("home_score").cast("int"),
        col("away_score").cast("int"),
        col("stadium")
    )

    players_clean = players.select(
        col("player_id").cast("int"),
        col("player_name"),
        col("team"),
        col("position")
    )

    events_clean = events.select(
        col("match_id").cast("int"),
        col("minute").cast("int"),
        col("player_id").cast("int"),
        col("event_type"),
        col("event_detail")
    )

    # Write cleaned parquet
    matches_out = f"{output_path}/matches"
    players_out = f"{output_path}/players"
    events_out = f"{output_path}/events"
    matches_clean.write.mode("overwrite").parquet(matches_out)
    players_clean.write.mode("overwrite").parquet(players_out)
    events_clean.write.mode("overwrite").parquet(events_out)
    print(f"Wrote matches to {matches_out}, players to {players_out} and events to {events_out}")

    # Sample aggregate: top scorers (events where event_type = 'goal')
    goals = events_clean.filter(col("event_type") == "goal")
    top_scorers = goals.groupBy("player_id").agg(_count("*").alias("goals")).orderBy(col("goals").desc())
    top_scorers.write.mode("overwrite").parquet(f"{output_path}/top_scorers")

    # Sample aggregate: team goals per season (join events -> players -> matches)
    events_with_player = goals.join(players_clean, on="player_id", how="left")
    matches_with_season = matches_clean.select("match_id", "season", "home_team", "away_team")
    goals_with_season = events_with_player.join(matches_with_season, on="match_id", how="left")
    # normalize team column from players table (team) -> use that as team
    team_goals = goals_with_season.groupBy("season", "team").agg(_count("*").alias("team_goals")).orderBy("season", "team_goals")
    team_goals.write.mode("overwrite").parquet(f"{output_path}/team_goals")

    print("Aggregates written. Done.")
    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--matches", default="data/raw/matches.csv")
    parser.add_argument("--players", default="data/raw/players.csv")
    parser.add_argument("--events", default="data/raw/events.csv")
    parser.add_argument("--output", default="data/processed")
    args = parser.parse_args()
    main(args.matches, args.players, args.events, args.output)
