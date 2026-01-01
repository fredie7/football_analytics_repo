# Import dependencies
from pyspark.sql.functions import col, lit, current_timestamp, when, lit
from delta.tables import DeltaTable

# Load data from the delta lake
silver_season = spark.read \
  .format("delta") \
      .load("abfss://silver@footballanalyticstorage.dfs.core.windows.net/dim_season")

%md
Creste delta live table for the season

%sql
CREATE TABLE IF NOT EXISTS dim_season (
  season_id INT,
  season_name STRING,
  season STRING,
  season_start_date STRING,
  season_end_date STRING,
  current_matchday LONG,
  winner STRING,
  source STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
-- LOCATION " "
# Implement SCD Type-1 for the season table
dim_season = DeltaTable.forName(spark, "dim_season")
(
    dim_season.alias("target")
    .merge(
        silver_season.alias("source"),
        "target.season_id = source.season_id"
    )
    .whenMatchedUpdate(
        set={
            "season_name": "source.season_name",
            "season": "source.season",
            "season_start_date": "source.season_start_date",
            "season_end_date": "source.season_end_date",
            "current_matchday": "source.current_matchday",
            "winner": "source.winner",
            "source": "source.source",
            "updated_at": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(
        values={
            "season_id": "source.season_id",
            "season_name": "source.season_name",
            "season": "source.season",
            "season_start_date": "source.season_start_date",
            "season_end_date": "source.season_end_date",
            "current_matchday": "source.current_matchday",
            "winner": "source.winner",
            "source": "source.source",
            "created_at": "current_timestamp()",
        }
    )
    .execute()
)

%md
# Test for the implementation for SCD Type-1
silver_season = (
    silver_season
    .withColumn(
        "season",
        when(
            col("season_id") == "2403",
            lit("2026")
        ).otherwise(col("season")
        )
    )
)
# Confirm changes
display(silver_season)

# Confirm changes
display(silver_season)

# Write the data to delta lake of the season's gold layer
dim_season.toDF() \
    .write \
        .format("delta") \
           .mode("overwrite") \
               .save("abfss://gold@footballanalyticstorage.dfs.core.windows.net/dim_season")

