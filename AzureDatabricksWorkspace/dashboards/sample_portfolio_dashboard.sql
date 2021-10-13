-- Databricks notebook source
-- MAGIC %python
-- MAGIC baslik = """<h2 style="color: #2e6c80; text-align: center;">&Ouml;rnek portf&ouml;y ile oluşturulmuş dashboard</h2><hr /><p style="text-align: center;">Secilen market ve varlıklar:</p><table style="height: 129px; width: 40.5064%; border-collapse: collapse; margin-left: auto; margin-right: auto;" border="1"><tbody><tr><td style="text-align: center;">1</td><td>KRAKEN  </td><td>BADGER</td></tr><tr><td style="text-align: center;">2</td><td>KRAKEN  </td><td>DOT</td></tr><tr><td style="text-align: center;">3</td><td>COINBASE</td><td>LTC</td></tr><tr><td style="text-align: center;">4</td><td>COINBASE</td><td>DOGE</td></tr><tr><td style="text-align: center;">5</td><td>BITFINEX</td><td>ETH</td></tr><tr><td style="text-align: center;">6</td><td>BITTREX </td><td>BTC</td></tr></tbody></table>"""
-- MAGIC displayHTML(baslik)

-- COMMAND ----------

create or replace temp view v_sample_portfolio
as
select s.exchange_id, s.base_asset from db_presentation.daily_summary s where 
(s.exchange_id = 'KRAKEN' and s.base_asset = 'BADGER') or
(s.exchange_id = 'KRAKEN' and s.base_asset = 'DOT') or
(s.exchange_id = 'COINBASE' and s.base_asset = 'LTC') or
(s.exchange_id = 'COINBASE' and s.base_asset = 'DOGE') or
(s.exchange_id = 'BITFINEX' and s.base_asset = 'ETH') or
(s.exchange_id = 'BITTREX' and s.base_asset = 'BTC') 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### a1-Fiyat değişimi son 1 saat ve 24 saat içinde %5 oranında düşen varlıklar bilgisi

-- COMMAND ----------

select s.exchange_id as Market, s.base_asset as Valık, case when s.change_1h < -5 then s.change_1h else '-' end as Son_1_saat, case when s.change_24h < -5 then s.change_24h else '-' end as Son_24_saat from db_presentation.daily_summary s where (s.change_1h <= -5 or s.change_24h <= -5) and (s.exchange_id, s.base_asset) in (select * from v_sample_portfolio) order by change_1h asc, change_24h asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### a2-Fiyat bilgisi hedef bir değer üzerine çıkan varlıklar bilgisi (Hedef değer: _30,000 USD_ )

-- COMMAND ----------

select s.exchange_id as Market, s.base_asset as Valık, s.current_price as Fiyat from db_presentation.daily_summary s where (s.current_price > 30000) and (s.exchange_id, s.base_asset) in (select * from v_sample_portfolio) order by s.current_price desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### a3-Son 24 saat içerisinde en fazla kayıp olan varlık için, bu kaybın oluştuğu an

-- COMMAND ----------

select s.exchange_id as Market, s.base_asset as Valık, s.change_24h as Gunluk_kayıp, s.min_time as Kaybın_oldugu_an from db_presentation.daily_summary s where (s.change_24h = (select min(a.change_24h) from db_presentation.daily_summary a where (a.exchange_id, a.base_asset) in (select * from v_sample_portfolio) )) and (s.exchange_id, s.base_asset) in (select * from v_sample_portfolio) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### a4-Kaybın oluştuğu an için takip ettiğiniz varlıkların her birinin saatlik ortalama fiyat değişimi

-- COMMAND ----------

select avg(d.change_1h) as Varlıkların_saatlik_ortalama_fiyat_degisimi from db_presentation.daily d where (d.price_time = (select s.min_time from db_presentation.daily_summary s where (s.change_24h = (select min(a.change_24h) from db_presentation.daily_summary a where (a.exchange_id, a.base_asset) in (select * from v_sample_portfolio) )) and (s.exchange_id, s.base_asset) in (select * from v_sample_portfolio))) and (d.exchange_id, d.base_asset) in (select * from v_sample_portfolio) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### a5-Son 24 saat içerisinde fiyat değişimi olarak en fazla dalgalanma gerçekleşen (min-max farkı olan) varlık ve bu kaybın oluştuğu an

-- COMMAND ----------

select s.exchange_id as Market, s.base_asset as Valık, s.min_price as En_dusuk_fiyat, s.max_price as En_yuksek_fiyat, ((s.max_price - s.min_price)*100/s.min_price) Dalgalanma_yuzdesi, s.min_time as Kaybın_oldugu_an from db_presentation.daily_summary s where ((s.max_price - s.min_price)/s.min_price = (select max((a.max_price - a.min_price)/a.min_price) from db_presentation.daily_summary a where (a.exchange_id, a.base_asset) in (select * from v_sample_portfolio) )) and (s.exchange_id, s.base_asset) in (select * from v_sample_portfolio) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### g1-Anlık portföy değer bilgisi (USD Pie Chart)

-- COMMAND ----------

select concat(s.exchange_id, ' - ', s.base_asset) as Varlık, s.current_price as Fiyat from db_presentation.daily_summary s where (s.exchange_id, s.base_asset) in (select * from v_sample_portfolio) order by s.current_price desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### g2-Portföydeki son 24 saat içerisinde en çok artış gösteren varlığın zaman/fiyat grafiği

-- COMMAND ----------

select concat(s.exchange_id, ' - ', s.base_asset) as En_cok_artan_varlık, s.change_24h as Artıs_yuzdesi from db_presentation.daily_summary s where s.change_24h = (select max(a.change_24h) from db_presentation.daily_summary a where (a.exchange_id, a.base_asset) in (select * from v_sample_portfolio)) and (s.exchange_id, s.base_asset) in (select * from v_sample_portfolio) 

-- COMMAND ----------

select concat(d.exchange_id, ' - ', d.base_asset) as Varlık, d.price as Fiyat, d.price_time as Zaman from db_presentation.daily d where (d.exchange_id, d.base_asset) in (select s.exchange_id, s.base_asset from db_presentation.daily_summary s where s.change_24h = (select max(a.change_24h) from db_presentation.daily_summary a where (a.exchange_id, a.base_asset) in (select * from v_sample_portfolio)) and (s.exchange_id, s.base_asset) in (select * from v_sample_portfolio) )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### g3-Portföydeki son 24 saat içerisinde en çok kayıp gösteren varlığın zaman/fiyat grafiği

-- COMMAND ----------

select concat(s.exchange_id, ' - ', s.base_asset) as En_az_artan_varlık, s.change_24h as Artıs_yuzdesi from db_presentation.daily_summary s where s.change_24h = (select min(a.change_24h) from db_presentation.daily_summary a where (a.exchange_id, a.base_asset) in (select * from v_sample_portfolio)) and (s.exchange_id, s.base_asset) in (select * from v_sample_portfolio) 

-- COMMAND ----------

select concat(d.exchange_id, ' - ', d.base_asset) as Varlık, d.price as Fiyat, d.price_time as Zaman from db_presentation.daily d where (d.exchange_id, d.base_asset) in (select s.exchange_id, s.base_asset from db_presentation.daily_summary s where s.change_24h = (select min(a.change_24h) from db_presentation.daily_summary a where (a.exchange_id, a.base_asset) in (select * from v_sample_portfolio)) and (s.exchange_id, s.base_asset) in (select * from v_sample_portfolio) )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### g4-Portföydeki tüm varlıkların USD toplamının son 24 saat içerisindeki zaman/fiyat grafiği

-- COMMAND ----------

select 'Tum_varlıklar', sum(d.price) as Toplam_fiyat, d.price_time as Zaman from db_presentation.daily d where (d.exchange_id, d.base_asset) in (select * from v_sample_portfolio) group by d.price_time
