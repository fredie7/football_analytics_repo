# Import required libraries
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp, col, when, sha2, concat_ws

# Create delta table for managers
%sql
CREATE TABLE IF NOT EXISTS dim_managers (
    manager_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    manager_id INT,
    club_id INT,
    manager_name STRING,
    date_of_birth DATE,
    manager_nationality STRING,
    contract_start_date DATE,
    contract_end_date DATE,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    ingestion_ts TIMESTAMP,
    source STRING,
     record_hash STRING
)
USING DELTA
-- LOCATION "abfss://gold@footballanalyticstorage.dfs.core.windows.net/dim_manager";


# Read managers' data from the delta lake 
silver_managers = spark.read \
    .format("delta") \
        .load("abfss://silver@footballanalyticstorage.dfs.core.windows.net/dim_managers")

# Perform SCD Type-2 upsert

# Define an object for the existing table
dim_managers = DeltaTable.forName(spark, 'dim_managers')

# For the update operation,

# Define the business key
business_key = "manager_id"

# Define the columns that should be compared before updating
compare_cols = [
    "club_id",
    "manager_name",
    "date_of_birth",
    "manager_nationality",
    "contract_start_date",
    "contract_end_date",
    "record_hash"
]

# Define the conditions for updating the existing table
change_conditions = " OR ".join([
    f"(target.{c} != source.{c} OR " 
    f"(target.{c} IS NULL AND source.{c} IS NOT NULL) OR "
    f"(target.{c} IS NOT NULL AND source.{c} IS NULL))"
    for c in compare_cols
])

# Perform update
dim_managers.alias("target").merge(
    silver_managers.alias("source"),
    f"""
    target.{business_key} = source.{business_key}
    AND target.is_current = true
    AND ({change_conditions})
    """
).whenMatchedUpdate(
    set={
        "valid_to": "current_timestamp()",
        "is_current": "false",
        "ingestion_ts": "current_timestamp()"
    }
).execute()

# For insert operation,

# Get the new player
new_manager = (
    silver_managers
    .withColumn("valid_from", current_timestamp())
    .withColumn("valid_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source", lit("silver_managers")) 
)

# Perform insert
dim_managers.alias("target").merge(
    new_manager.alias("source"),
    f"target.{business_key} = source.{business_key} AND target.is_current = true"
).whenNotMatchedInsert(
    values={c: f"source.{c}" for c in new_manager.columns}
).execute()

# Peek at the tabke
display(spark.table("dim_managers"))

# Test for SCD Type-2
silver_managers = (
    silver_managers
    .withColumn(
        "manager_name",
        when(col("manager_id") == 11323, "Filipe Scolari")
        .otherwise(col("manager_name"))
    )
    .withColumn(
        "manager_nationality",
        when(col("manager_id") == 11323, "Spain")
        .otherwise(col("manager_nationality"))
    )
    .withColumn(
        "record_hash",
    sha2(
        concat_ws(
            "||",
            col("manager_id"),
            col("club_id"),
            col("manager_name"),
            col("date_of_birth"),
            col("contract_start_date"),
            col("contract_end_date")
        ),
        256
    )
    )
)

# %md
# Data Insertion test

%sql
SELECT * FROM dim_managers WHERE manager_id = 11323;
# -- SELECT * FROM dim_managers WHERE manager_id = 179744;

%sql
SELECT * FROM dim_managers WHERE manager_id = 1011;

# spark.catalog.clearCache()

# Write the data to delta lake
dim_managers.toDF() \
    .write \
        .format("delta") \
            .mode("overwrite") \
                .save("abfss://gold@footballanalyticstorage.dfs.core.windows.net/dim_managers")
