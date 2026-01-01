from pyspark.sql.functions import col, lit, current_timestamp,when
from delta.tables import DeltaTable

%sql
CREATE TABLE IF NOT EXISTS dim_clubs(
  club_id STRING,
  football_club STRING,
  club_abbreviation STRING,
  club_logo STRING,
  club_address STRING,
  club_website STRING,
  founded INTEGER,
  venue STRING,
  data_source STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
-- LOCATION " "

# Read existing data from the silver layer of the delta lake
silver_clubs = spark.read \
    .format("delta") \
        .load("abfss://silver@footballanalyticstorage.dfs.core.windows.net/dim_football_clubs")

dim_clubs = DeltaTable.forName(spark, "dim_clubs")

(
    dim_clubs.alias("target")
    .merge(
        silver_clubs.alias("source"),
        "target.club_id = source.club_id"
    )
    .whenMatchedUpdate(
        set={
            "football_club": "source.football_club",
            "club_abbreviation": "source.club_abbreviation",
            "club_logo": "source.club_logo",
            "club_address": "source.club_address",
            "club_website": "source.club_website",
            "founded": "source.founded",
            "venue": "source.venue",
            "data_source": "source.data_source",
            "updated_at": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(
        values={
            "club_id": "source.club_id",
            "football_club": "source.football_club",
            "club_abbreviation": "source.club_abbreviation",
            "club_logo": "source.club_logo",
            "club_address": "source.club_address",
            "club_website": "source.club_website",
            "founded": "source.founded",
            "venue": "source.venue",
            "data_source": "source.data_source",
            "created_at": "current_timestamp()"
        }
    )
    .execute()
)

# Implement SCD Type-1
dim_clubs = DeltaTable.forName(spark, "dim_clubs")

(
    dim_clubs.alias("target")
    .merge(
        silver_clubs.alias("source"),
        "target.club_id = source.club_id"
    )
    .whenMatchedUpdate(
        set={
            "football_club": "source.football_club",
            "club_abbreviation": "source.club_abbreviation",
            "club_logo": "source.club_logo",
            "club_address": "source.club_address",
            "club_website": "source.club_website",
            "founded": "source.founded",
            "venue": "source.venue",
            "data_source": "source.data_source",
            "updated_at": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(
        values={
            "club_id": "source.club_id",
            "football_club": "source.football_club",
            "club_abbreviation": "source.club_abbreviation",
            "club_logo": "source.club_logo",
            "club_address": "source.club_address",
            "club_website": "source.club_website",
            "founded": "source.founded",
            "venue": "source.venue",
            "data_source": "source.data_source",
            "created_at": "current_timestamp()"
        }
    )
    .execute()
)

# Tests and review
# Update the venue for club_id = 64
silver_clubs = (
    silver_clubs
    .withColumn(
        "venue",
        when(
            col("club_id") == 64,
            "upton park"
        ).otherwise(col("venue"))
    )
)

# Confirlm changes
%sql
select * from dim_clubs where club_id = 64

# Write data to the delta lake gold layer
dim_clubs.toDF() \
    .write \
       .format("delta") \
          .mode("overwrite") \
               .save("abfss://gold@footballanalyticstorage.dfs.core.windows.net/dim_football_clubs")
