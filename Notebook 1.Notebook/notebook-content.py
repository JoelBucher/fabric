# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2d1285f7-9dab-4305-be48-4ae1c5350450",
# META       "default_lakehouse_name": "SpotifyLakehouse",
# META       "default_lakehouse_workspace_id": "c462b9ce-413d-49f5-8706-051e7dcc9833",
# META       "known_lakehouses": [
# META         {
# META           "id": "2d1285f7-9dab-4305-be48-4ae1c5350450"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Spotify Dataset

# MARKDOWN ********************

# ## Songs by Artist
# Find all songs from the artist "Caravan Palace"

# CELL ********************

df = spark.sql("SELECT * FROM SpotifyLakehouse.dataset WHERE artists ILIKE 'Caravan Palace' LIMIT 10")
display(df)
hey there

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Similar Songs
# Find songs similar to the song "Lone Digger" by Caravan Palace.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col

spark.table("SpotifyLakehouse.dataset")
# Assuming your DataFrame is called 'songs'
# Step 1: Compute the average features for 'Caravan Palace'

songs = spark.table("SpotifyLakehouse.dataset")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cp = songs.filter(songs.track_name == 'Lone Digger') \
          .filter(songs.artists == 'Caravan Palace') \
          .agg(
              F.avg('danceability').alias('d'),
              F.avg('energy').alias('e'),
              F.avg('valence').alias('v'),
              F.avg('tempo').alias('t'),
              F.avg('liveness').alias('l'),
              F.avg('popularity').alias('p')
          ).collect()[0]  # collect to get the values

cp_d, cp_e, cp_v, cp_t, cp_l, cp_p = cp['d'], cp['e'], cp['v'], cp['t'], cp['l'], cp['p']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

artist_distances = songs \
    .withColumn(
        'distance',
        F.sqrt(
            F.pow(F.col('danceability') - cp_d, 2) +
            F.pow(F.col('energy') - cp_e, 2) +
            F.pow(F.col('valence') - cp_v, 2) +
            F.pow(F.col('tempo') - cp_t, 2) +
            F.pow(F.col('liveness') - cp_l, 2) + 
            F.pow(F.col('popularity') - cp_p, 2)
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4: Order and take top 20
ordered_desc = artist_distances \
    .filter(col('distance').isNotNull()) \
    .orderBy('distance')
    
top_20 = ordered_desc.limit(20)
display(top_20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
