# Databricks notebook source
from pyspark.sql.functions import lit

# COMMAND ----------

asset_df = spark.read.table('db_processed.assets')

# COMMAND ----------

daily_df = (asset_df.select(asset_df.exchange_id, 
                             asset_df.base_asset, 
                             asset_df.price, 
                             asset_df.change_24h, 
                             asset_df.ingested_at.alias('price_time'))
                             .withColumn("change_1h", lit('0').cast('float'))
           )

# COMMAND ----------

daily_df.printSchema()

# COMMAND ----------

daily_df.write.mode("overwrite").format("parquet").saveAsTable("db_presentation.daily")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT to DELTA db_presentation.daily

# COMMAND ----------

daily_summary_df = (asset_df.select(asset_df.exchange_id, 
                                    asset_df.base_asset, 
                                    asset_df.price.alias('current_price'), 
                                    asset_df.ingested_at.alias('price_time'),
                                    asset_df.change_24h.alias('change_1h'),
                                    asset_df.change_24h,
                                    asset_df.price.alias('min_price'),
                                    asset_df.ingested_at.alias('min_time'),
                                    asset_df.price.alias('max_price'),
                                    asset_df.ingested_at.alias('max_time')
                                   )
           )

# COMMAND ----------

daily_summary_df.printSchema()

# COMMAND ----------

daily_summary_df.write.mode("overwrite").format("parquet").saveAsTable("db_presentation.daily_summary")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT to DELTA db_presentation.daily_summary
