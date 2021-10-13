-- Databricks notebook source
truncate table db_presentation.daily_summary

-- COMMAND ----------

insert into
  db_presentation.daily_summary
select
  e.exchange_id,
  e.base_asset,
  e.price as current_price,
  e.price_time,
  e.change_1h,
  e.change_24h,
  f.min_price,
  f.min_time,
  g.max_price,
  g.max_time
from
  db_presentation.daily e
  join (
    select
      d2.exchange_id,
      d2.base_asset,
      d3.min_price,
      min(d2.price_time) as min_time
    from
      db_presentation.daily d2
      join (
        select
          d1.exchange_id,
          d1.base_asset,
          min(d1.price) as min_price,
          max(d1.price) as max_price
        from
          db_presentation.daily d1
        group by
          d1.exchange_id,
          d1.base_asset
      ) d3 on d2.exchange_id = d3.exchange_id
      and d2.base_asset = d3.base_asset
      and d2.price = d3.min_price
    group by
      d2.exchange_id,
      d2.base_asset,
      d3.min_price
  ) f on e.exchange_id = f.exchange_id
  and e.base_asset = f.base_asset
  join (
    select
      d4.exchange_id,
      d4.base_asset,
      d5.max_price,
      min(d4.price_time) as max_time
    from
      db_presentation.daily d4
      join (
        select
          d1.exchange_id,
          d1.base_asset,
          min(d1.price) as min_price,
          max(d1.price) as max_price
        from
          db_presentation.daily d1
        group by
          d1.exchange_id,
          d1.base_asset
      ) d5 on d4.exchange_id = d5.exchange_id
      and d4.base_asset = d5.base_asset
      and d4.price = d5.max_price
    group by
      d4.exchange_id,
      d4.base_asset,
      d5.max_price
  ) g on e.exchange_id = g.exchange_id
  and e.base_asset = g.base_asset
where
  e.price_time = (
    select
      max(h.price_time)
    from
      db_presentation.daily h
  )
