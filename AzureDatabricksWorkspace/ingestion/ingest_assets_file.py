# Databricks notebook source
# MAGIC %md #### -.-.- Cryptingup servis çağrısı sonucu oluşan json dosyasının şeması tanımlanır -.-.-

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, ArrayType

# COMMAND ----------

asset_schema = StructType(fields=[StructField("exchange_id", StringType(), False), 
								  StructField("symbol", StringType(), False), 
								  StructField("base_asset", StringType(), False),
								  StructField("quote_asset", StringType(), False),
								  StructField("price_unconverted", FloatType(), True), 
								  StructField("price", FloatType(), False),
								  StructField("change_24h", FloatType(), False),
								  StructField("spread", FloatType(), True),
								  StructField("volume_24h", FloatType(), True),
								  StructField("status", StringType(), True),
								  StructField("created_at",TimestampType(), True),
								  StructField("updated_at", TimestampType(), False)
								  ]) 

# COMMAND ----------

getallassetsresp_schema = StructType(fields=[StructField("markets", ArrayType(asset_schema), True),
											 StructField("next", StringType(), True)
											 ])

# COMMAND ----------

# MAGIC %md #### -.-.- Şema verilerek dosya data frame'e alınır -.-.-

# COMMAND ----------

getallassets_df = spark.read.schema(getallassetsresp_schema).option("multiLine", True).json("/mnt/dataenggpprjdl/raw/cryptingupAllAssetsUSD.json")

# COMMAND ----------

# getallassets_df.printSchema()

# COMMAND ----------

# MAGIC %md #### -.-.- Veri tabanına kaydedilecek data frame oluşturulur -.-.-

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, explode, col

# COMMAND ----------

asset_df = ( getallassets_df.select("*", explode("markets").alias("marketitem"))
                            .select("*", "marketitem.*")
                            .withColumn("ingested_at", current_timestamp())
                            .drop(col("next")) 
                            .drop(col("markets"))
                            .drop(col("marketitem"))
                            .drop(col("created_at"))
                            .drop(col("price_unconverted"))
                            )


# COMMAND ----------

# display(asset_df)

# COMMAND ----------

# MAGIC %md #### -.-.- Veriler assets tablosuna append edilir, new_assets tablosu overwrite edilir -.-.-

# COMMAND ----------

asset_df.write.mode("append").format("delta").saveAsTable("db_processed.assets")

# COMMAND ----------

asset_df.write.mode("overwrite").format("delta").saveAsTable("db_processed.new_assets")
