-- Databricks notebook source
-- MAGIC %python
-- MAGIC baslik = """<h2 style="color: #2e6c80; text-align: center;">Tek marketli dashboard &ouml;rneği</h2><hr><p style="text-align: center;">Secilen market: <em><strong>KRAKEN</strong></em></p>"""
-- MAGIC displayHTML(baslik)

-- COMMAND ----------

create or replace temp view v_sample_exchange
as
select s.exchange_id from db_presentation.daily_summary s where s.exchange_id = 'KRAKEN'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### a1-Fiyat değişimi son 1 saat ve 24 saat içinde %5 oranında düşen varlıklar bilgisi

-- COMMAND ----------

select s.exchange_id as Market, s.base_asset as Valık, case when s.change_1h < -5 then s.change_1h else '-' end as Son_1_saat, case when s.change_24h < -5 then s.change_24h else '-' end as Son_24_saat from db_presentation.daily_summary s where (s.change_1h <= -5 or s.change_24h <= -5) and s.exchange_id in (select * from v_sample_exchange) order by change_1h asc, change_24h asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### a2-Fiyat bilgisi hedef bir değer üzerine çıkan varlıklar bilgisi (Hedef değer: _30,000 USD_ )

-- COMMAND ----------

select s.exchange_id as Market, s.base_asset as Valık, s.current_price as Fiyat from db_presentation.daily_summary s where (s.current_price > 30000) and s.exchange_id in (select * from v_sample_exchange) order by s.current_price desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### a3-Son 24 saat içerisinde en fazla kayıp olan varlık için, bu kaybın oluştuğu an

-- COMMAND ----------

select s.exchange_id as Market, s.base_asset as Valık, s.change_24h as Gunluk_kayıp, s.min_time as Kaybın_oldugu_an from db_presentation.daily_summary s where (s.change_24h = (select min(a.change_24h) from db_presentation.daily_summary a where a.exchange_id in (select * from v_sample_exchange) )) and s.exchange_id in (select * from v_sample_exchange) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### a4-Kaybın oluştuğu an için takip ettiğiniz varlıkların her birinin saatlik ortalama fiyat değişimi

-- COMMAND ----------

select avg(d.change_1h) as Varlıkların_saatlik_ortalama_fiyat_degisimi from db_presentation.daily d where (d.price_time = (select s.min_time from db_presentation.daily_summary s where (s.change_24h = (select min(a.change_24h) from db_presentation.daily_summary a where a.exchange_id in (select * from v_sample_exchange) )) and s.exchange_id in (select * from v_sample_exchange))) and d.exchange_id in (select * from v_sample_exchange) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### a5-Son 24 saat içerisinde fiyat değişimi olarak en fazla dalgalanma gerçekleşen (min-max farkı olan) varlık ve bu kaybın oluştuğu an

-- COMMAND ----------

select s.exchange_id as Market, s.base_asset as Valık, s.min_price as En_dusuk_fiyat, s.max_price as En_yuksek_fiyat, ((s.max_price - s.min_price)*100/s.min_price) Dalgalanma_yuzdesi, s.min_time as Kaybın_oldugu_an from db_presentation.daily_summary s where ((s.max_price - s.min_price)/s.min_price = (select max((a.max_price - a.min_price)/a.min_price) from db_presentation.daily_summary a where a.exchange_id in (select * from v_sample_exchange) )) and s.exchange_id in (select * from v_sample_exchange) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### g1-Anlık portföy değer bilgisi (USD Pie Chart)

-- COMMAND ----------

select concat(s.exchange_id, ' - ', s.base_asset) as Varlık, s.current_price as Fiyat from db_presentation.daily_summary s where s.exchange_id in (select * from v_sample_exchange) order by s.current_price desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### g2-Portföydeki son 24 saat içerisinde en çok artış gösteren varlığın zaman/fiyat grafiği

-- COMMAND ----------

select concat(s.exchange_id, ' - ', s.base_asset) as En_cok_artan_varlık, s.change_24h as Artıs_yuzdesi from db_presentation.daily_summary s where s.change_24h = (select max(a.change_24h) from db_presentation.daily_summary a where a.exchange_id in (select * from v_sample_exchange)) and s.exchange_id in (select * from v_sample_exchange) 

-- COMMAND ----------

select * from db_presentation.daily_summary a where a.exchange_id in (select * from v_sample_exchange) and a.base_asset = 'APHA'

-- COMMAND ----------

select concat(d.exchange_id, ' - ', d.base_asset) as Varlık, d.price as Fiyat, d.price_time as Zaman from db_presentation.daily d where (d.exchange_id, d.base_asset) in (select s.exchange_id, s.base_asset from db_presentation.daily_summary s where s.change_24h = (select max(a.change_24h) from db_presentation.daily_summary a where a.exchange_id in (select * from v_sample_exchange)) and s.exchange_id in (select * from v_sample_exchange) )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### g3-Portföydeki son 24 saat içerisinde en çok kayıp gösteren varlığın zaman/fiyat grafiği

-- COMMAND ----------

select concat(s.exchange_id, ' - ', s.base_asset) as En_cok_artan_varlık, s.change_24h as Artıs_yuzdesi from db_presentation.daily_summary s where s.change_24h = (select min(a.change_24h) from db_presentation.daily_summary a where a.exchange_id in (select * from v_sample_exchange)) and s.exchange_id in (select * from v_sample_exchange) 

-- COMMAND ----------

select concat(d.exchange_id, ' - ', d.base_asset) as Varlık, d.price as Fiyat, d.price_time as Zaman from db_presentation.daily d where (d.exchange_id, d.base_asset) in (select s.exchange_id, s.base_asset from db_presentation.daily_summary s where s.change_24h = (select min(a.change_24h) from db_presentation.daily_summary a where a.exchange_id in (select * from v_sample_exchange)) and s.exchange_id in (select * from v_sample_exchange) )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### g4-Portföydeki tüm varlıkların USD toplamının son 24 saat içerisindeki zaman/fiyat grafiği

-- COMMAND ----------

select 'Tum_varlıklar', sum(d.price) as Toplam_fiyat, d.price_time as Zaman from db_presentation.daily d where d.exchange_id in (select * from v_sample_exchange) group by d.price_time
