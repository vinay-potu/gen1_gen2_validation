// Databricks notebook source
// MAGIC %run /Shared/final_validation

// COMMAND ----------


spark.conf.set("spark.databricks.delta.formatCheck.enabled","false")
spark.read.parquet("dbfs:/dbfs/FileStorage/validation").createOrReplaceTempView("T1")
spark.read.parquet("dbfs:/dbfs/FileStorage/validation_phase2").createOrReplaceTempView("T2")
spark.sql("""
select distinct 
case when dag_id like '%_daily%' then  split(dag_id,'_daily')[0]|| '_daily' else 
case when dag_id like '%_weekly%' then  split(dag_id,'_monthly')[0]|| '_weekly' else dag_id end end
as dag_id,
gen1_path,gen2_path from 
(
select distinct dag_id,gen1_path,gen2_path from T1 where  dag_id not like '%streaming%' and  dag_id not like '%test%' and  gen1_path not like '%test%' and gen2_path not like '%test%' and gen1_count >0 and gen2_count >0 
union
select distinct dag_id,gen1_path,gen2_path from T2 where  dag_id not like '%streaming%' and dag_id not like '%test%' and  gen1_path not like '%test%' and gen2_path not like '%test%' and gen1_count >0 and gen2_count >0 and env='prod'
) where dag_id like '%daily' or  dag_id like '%weekly'
""").createOrReplaceTempView("T3")

// COMMAND ----------

def getTableListForValidation(df:DataFrame):String={
  val tables=new StringBuilder
  df.collectAsList.forEach{x=>
   tables.append(s"${x.getString(0)}\n")
  }
tables.toString
}

// COMMAND ----------

val df = spark.sql("select concat_ws(',','prod',dag_id,gen1_path,gen2_path) from T3")

validation(getTableListForValidation(df))

// COMMAND ----------

val df = spark.sql("select concat_ws(',','prod',dag_id,gen1_path,gen2_path) from T3 where gen2_path not in(select distinct gen2_path from default.validation_final where ts like '%2023-09-19%') ")
validation(getTableListForValidation(df))

// COMMAND ----------

// MAGIC %sql select 
// MAGIC distinct 
// MAGIC gen2_path
// MAGIC -- ,gen1_except_gen2
// MAGIC -- ,gen2_except_gen1 
// MAGIC from default.validation_final where 
// MAGIC except_validation="Failed" 
// MAGIC and gen1_count != 0 
// MAGIC -- and gen1_except_gen2 = gen2_except_gen1 --order by ts desc

// COMMAND ----------

// MAGIC %sql 
// MAGIC select concat_ws(',','prod',dag_id,gen1_path,gen2_path) from default.validation_final where except_validation="Failed"
