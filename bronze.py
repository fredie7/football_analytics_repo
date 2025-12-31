from pyspark.sql.functions import explode, col, collect_list, struct, array_distinct

# load league standing raw data
raw_league_standing = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/standings"
)

# Flatten the data by turning arrays into rows
flattened_raw_standing = (
    raw_league_standing
    .withColumn("standing", explode(col("standings")))
    .withColumn("team_info", explode(col("standing.table")))
)

# Extract selected columns
bronze_league_standing = flattened_raw_standing.select(
    col("team_info.position"),
    col("team_info.team.id"),
    col("team_info.team.name"),
    col("team_info.team.shortName"),
    col("team_info.team.tla"),
    col("team_info.team.crest"),
    col("team_info.playedGames"),
    col("team_info.won"),
    col("team_info.draw"),
    col("team_info.lost"),
    col("team_info.points"),
    col("team_info.goalsFor"),
    col("team_info.goalsAgainst"),
    col("team_info.goalDifference")
)

# Write to delta lake
bronze_league_standing.write \
  .format("delta") \
    .mode("overwrite") \
        .save("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/league_standing")

# Prepare teams data for bronze layer
raw_teams = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/teams"
)

# Flatten teams array
teams_data = raw_teams.withColumn("team", explode(col("teams")))

Flatten squard array to get players
players_data = teams_data.withColumn(
    "player",
    explode(col("team.squad"))
)

# Aggregate players
players_agg_data = (
    players_data
    .groupBy(
        col("team.id"),
        col("team.name"),
        col("team.shortName"),
        col("team.tla"),
        col("team.crest"),
        col("team.founded"),
        col("team.venue"),
        col("team.address"),
        col("team.website"),
        col("team.coach.name").alias("coach"),
        col("team.coach.nationality")
    )
    .agg(
        collect_list(col("player.name")).alias("players")
    )
)

# Write team info to delta lake
bronze_teams.write \
    .format("delta") \
        .mode("overwrite") \
            .save("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/teams_info")

# Load clubs data
raw_club_info = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/club_info"
)

# Flatten the teams to get runninn competitions
teams_info = raw_club_info.withColumn("team", explode(col("teams")))

# Flatten running_competitions
retrieved_team_info = teams_info.withColumn("competition", explode(col("team.runningCompetitions")))

# Obtain relevant columns
clubs_data = (
    retrieved_team_info
    .groupBy(
        col("team.id").alias("id"),
        col("team.name").alias("name"),
        col("team.shortName").alias("shortName"),
        col("team.tla").alias("TLA"),
        col("team.crest").alias("crest"),
        col("team.address").alias("address"),
        col("team.website").alias("website"),
        col("team.founded").alias("founded"),
        col("team.clubColors").alias("clubColors"),
        col("team.venue").alias("venue")
    )
    .agg(
        array_distinct(collect_list(col("competition.name"))).alias("runningCompetitions")
    )
)

# Reassign relevant information
bronze_clubs_info = clubs_data

# Write clubs data to deltalake
bronze_clubs_info.write \
  .format("delta") \
    .mode("overwrite") \
        .save("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/clubs_info")

# load league players details
raw_players_details = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/player_details"
)

# Explode teams data
teams_exploded_data = (
    raw_players_details
        .select(explode(col("teams")).alias("team"))
)

# Collect relevant columns
bronze_players_details = (
    teams_exploded_data
        .select(
            col("team.id").alias("club_id"),
            col("team.name").alias("club_name"),
            explode(col("team.squad")).alias("player")
        )
        .select(
            col("player.id").alias("player_id"),
            col("player.name").alias("player_name"),
            col("player.position").alias("position"),
            col("player.dateOfBirth").alias("date_of_birth"),
            col("player.nationality").alias("nationality"),
            col("club_id"),
            col("club_name")
        )
)

# Write players' data to delta lake
bronze_players_details.write \
    .format("delta") \
        .mode("overwrite") \
            .save("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/players_details")

Load top scorers' data
raw_scorers_data = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/scorers"
)
# Flatten scorersarray to obtain eah team's scorers' info
scorers_data = (
    raw_scorers_data
        .select(explode(col("scorers")).alias("scorer"))
)

# Deconstruct to obtain relevant columns
bronze_top_scorers = (
    scorers_data
        .select(
            col("scorer.team.id").alias("club_id"),
            col("scorer.team.name").alias("football_club"),
            col("scorer.playedMatches"),
            col("scorer.goals"),
            col("scorer.assists"),
            col("scorer.penalties"),
            col("scorer.player.id").alias("player_id"),
            col("scorer.player.name").alias("player_name"),
            col("scorer.player.section")
        )
)

# Write top_scorers to deltalake
bronze_top_scorers.write \
    .format("delta") \
        .mode("overwrite") \
            .save("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/top_scorers")

# Load managers' data
raw_managers = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/teams"
)

# Explode teams array
managers_exploded_data = (
    raw_managers
        .select(explode(col("teams")).alias("team"))
)

# Select relevant columns
bronze_managers = (
    managers_exploded_data
        .select(
            col("team.coach.id").alias("manager_id"),
            col("team.id").alias("club_id"),
            col("team.coach.name").alias("manager_name"),
            col("team.coach.dateOfBirth").alias("date_of_birth"),
            col("team.coach.nationality"),
            col("team.coach.contract.start").alias("contract_start_date"),
            col("team.coach.contract.until").alias("contract_end_date")
        )
)

# Write managers' data to deltalake
bronze_managers.write \
    .format("delta") \
        .mode("append") \
            .save("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/managers_info")

# Obtain season in context
# Load data
raw_season = spark.read.format("json").load(
    "abfss://raw@footballanalyticstorage.dfs.core.windows.net/teams"
)

# Collect relevant columns
bronze_season = raw_season.select(
    col("season.id").alias("season_id"),
    col("competition.name").alias("season_name"),
    col("filters.season"),
    col("season.startDate").alias("season_start_date"),
    col("season.endDate").alias("season_end_date"),
    col("season.currentMatchday").alias("current_matchday"),
    col("season.winner").alias("winner")
)

# Write season data to delta lake
bronze_season.write \
    .format("delta") \
        .mode("overwrite") \
            .save("abfss://bronze@footballanalyticstorage.dfs.core.windows.net/season_info")
