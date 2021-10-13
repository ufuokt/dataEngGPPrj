-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### -.-.- Yeni çekilen asset verileri "new_assets" tablosundan "daily" tablosuna alınır. Bu arada 1 saatlik fiyat değişim yüzdesi de verilere eklenir -.-.-

-- COMMAND ----------

insert into
  db_presentation.daily
select
  na.exchange_id,
  na.base_asset,
  na.price,
  na.change_24h,
  na.ingested_at as price_time,
  oa.change_1h
from
  db_processed.new_assets na
  join (
    select
      n.exchange_id,
      n.base_asset,
      (n.price - o.price) * 100 / o.price as change_1h
    from
      db_processed.new_assets n
      join (
        select
          *
        from
          db_processed.assets
        where
          ingested_at = (
            select
              min(ingested_at)
            from
              db_processed.assets
            where
              ingested_at >= (
                select
                  max(ingested_at) - make_interval(0, 0, 0, 0, 1, 02, 00.000000)
                from
                  db_processed.assets
              )
          )
      ) o on n.exchange_id = o.exchange_id
      and n.base_asset = o.base_asset
  ) oa on na.exchange_id = oa.exchange_id
  and na.base_asset = oa.base_asset

-- COMMAND ----------

-- MAGIC %md #### -.-.- 24 saatten eski asset fiyat kayıtları "daily" tablosundan çıkarılır -.-.-

-- COMMAND ----------

delete from
  db_presentation.daily
where
  price_time < (
    select
      max(ingested_at) - make_interval(0, 0, 0, 1, 0, 2, 00.000000)
    from
      db_processed.new_assets
  )
