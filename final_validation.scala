// Databricks notebook source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructField}
import io.delta.tables._
import org.apache.spark.sql.functions._
import spark.implicits._
def setGen2NpiiScope() :Unit=
{
  val credentialScope = "datalake-gen2-npii-rw"
  val client_id = dbutils.secrets.get(scope = credentialScope, key = "client_id")
  val credential = dbutils.secrets.get(scope = credentialScope, key = "client_secret")
  spark.conf.set("fs.azure.account.auth.type.dap0prod0gen20datanpii.dfs.core.windows.net", "OAuth")
  spark.conf.set("fs.azure.account.oauth.provider.type.dap0prod0gen20datanpii.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.conf.set("fs.azure.account.oauth2.client.id.dap0prod0gen20datanpii.dfs.core.windows.net", client_id)
  spark.conf.set("fs.azure.account.oauth2.client.secret.dap0prod0gen20datanpii.dfs.core.windows.net", credential)
  spark.conf.set("fs.azure.account.oauth2.client.endpoint.dap0prod0gen20datanpii.dfs.core.windows.net", "https://login.microsoftonline.com/3698556c-48eb-4511-8a0e-5fb6b7ebb01f/oauth2/token")
}

def setGen1NpiiScope() :Unit=
{
  var client_id = dbutils.secrets.get(scope = "data-lake-scope", key = "client_id")
  var credential = dbutils.secrets.get(scope = "data-lake-scope", key = "credential")
  var directory_id = dbutils.secrets.get(scope = "data-lake-scope", key = "directory_id")
  var adls_url = dbutils.secrets.get(scope = "data-lake-scope", key = "adls_url")
  spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
  spark.conf.set("dfs.adls.oauth2.client.id", client_id)
  spark.conf.set("dfs.adls.oauth2.credential", credential)
  spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/3698556c-48eb-4511-8a0e-5fb6b7ebb01f/oauth2/token".format(directory_id))
}

def setGen1PiiScope() :Unit=
{
  var client_id = dbutils.secrets.get(scope = "pii-data-lake-scope", key = "client_id")
  var credential = dbutils.secrets.get(scope = "pii-data-lake-scope", key = "credential")
  var directory_id = dbutils.secrets.get(scope = "pii-data-lake-scope", key = "directory_id")
  var adls_url = dbutils.secrets.get(scope = "pii-data-lake-scope", key = "adls_url")
  spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
  spark.conf.set("dfs.adls.oauth2.client.id", client_id)
  spark.conf.set("dfs.adls.oauth2.credential", credential)
  spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/3698556c-48eb-4511-8a0e-5fb6b7ebb01f/oauth2/token".format(directory_id))
}

def setGen2PiiScope() :Unit=
{
   val credentialScope_pii = "datalake-gen2-pii-rw"
  val client_id_pii = dbutils.secrets.get(scope = credentialScope_pii, key = "client_id")
  val credential_pii = dbutils.secrets.get(scope = credentialScope_pii, key = "client_secret")
  spark.conf.set("fs.azure.account.auth.type.dap0prod0gen2pii0datapii.dfs.core.windows.net", "OAuth")
  spark.conf.set("fs.azure.account.oauth.provider.type.dap0prod0gen2pii0datapii.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.conf.set("fs.azure.account.oauth2.client.id.dap0prod0gen2pii0datapii.dfs.core.windows.net", client_id_pii)
  spark.conf.set("fs.azure.account.oauth2.client.secret.dap0prod0gen2pii0datapii.dfs.core.windows.net", credential_pii)
  spark.conf.set("fs.azure.account.oauth2.client.endpoint.dap0prod0gen2pii0datapii.dfs.core.windows.net", "https://login.microsoftonline.com/3698556c-48eb-4511-8a0e-5fb6b7ebb01f/oauth2/token")
}
setGen1NpiiScope
setGen2NpiiScope

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructField}

def getCleanedSchema(df: DataFrame): Map[String, (DataType, Boolean)] = {
    df.schema.map { (structField: StructField) =>
      structField.name.toLowerCase -> (structField.dataType, structField.nullable)
    }.toMap
  }

// Compare relevant information
def getSchemaDifference(schema1: Map[String, (DataType, Boolean)],
                        schema2: Map[String, (DataType, Boolean)]
                       ): Map[String, (Option[(DataType, Boolean)], Option[(DataType, Boolean)])] = {
  (schema1.keys ++ schema2.keys).
    map(_.toLowerCase).
    toList.distinct.
    flatMap { (columnName: String) =>
      val schema1FieldOpt: Option[(DataType, Boolean)] = schema1.get(columnName)
      val schema2FieldOpt: Option[(DataType, Boolean)] = schema2.get(columnName)

      if (schema1FieldOpt == schema2FieldOpt) None
      else Some(columnName -> (schema1FieldOpt, schema2FieldOpt))
    }.toMap
}

def schemaValiadation(gen1DF:DataFrame, gen2DF:DataFrame):
        Map[String,String] =
{
  var Column_level_null_Validation = "Failed"
  val diff = getSchemaDifference(getCleanedSchema(gen1DF),getCleanedSchema(gen2DF))
  if(diff.size >0)
      print(s"schemaValiadation failed diff=$diff")
  else
    Column_level_null_Validation = "Passed"
    print(s"schemaValiadation=$Column_level_null_Validation => ")
    
  return Map("isPassed" -> Column_level_null_Validation, "map" -> diff.toString())
}

def fileValidation(gen1_path:String,gen2_path:String):String=
{
    var res = "Failed"
    try
    {
        res = if(dbutils.fs.ls(gen1_path).map(_.path).filter(_.contains("parquet")).length >0 && dbutils.fs.ls(gen2_path).map(_.path).filter(_.contains("parquet")).length >0) "Passed" else "Failed"
    }catch 
    {
      case e: java.io.FileNotFoundException => { 
        println("ERROR:"+e.getMessage() )
        res = "Failed"
      }
    }
    print(s"fileValidation=$res => ")
    return res
}

def countValiadation(gen1DF:DataFrame,gen2DF:DataFrame):Map[String,String]=
{
  var countMatched = "Failed";
  //println("Doing count validation between the tables") 
  val gen1_cnt = gen1DF.count()
  val gen2_cnt = gen2DF.count()

  if (gen1_cnt == gen2_cnt) 
  {
    //println("count validation passed for both table")
    countMatched = "Passed"
  }
  print(s"countValiadation=$countMatched => ")
  return Map("isPassed" -> countMatched, "gen1_cnt" -> (gen1_cnt).toString , "gen2_cnt" -> (gen2_cnt).toString);
}

def validateDistinctCount(gen1DF:DataFrame, gen2DF:DataFrame,drop_col:Seq[String]):Map[String,String]=
{
  var Dis_Count_Validation = "Failed"
  // Distinct validation
  //println("Doing distinct validation between the tables")
  val gen1_dis_cnt= gen1DF.drop(drop_col:_*).distinct().count()
  val gen2_dis_cnt= gen2DF.drop(drop_col:_*).distinct().count()
  if (gen1_dis_cnt == gen1_dis_cnt) 
  {
    //println("Distinct validation passed for table")
    Dis_Count_Validation = "Passed"
  }
   print(s"distinctCountValidation=$Dis_Count_Validation => ")
  return Map("isPassed" -> Dis_Count_Validation, "gen1_dis_cnt" -> (gen1_dis_cnt).toString , "gen2_dis_cnt" -> (gen2_dis_cnt).toString);
}

def validateData(gen1DF:DataFrame, gen2DF:DataFrame,drop_col:Seq[String]):Map[String,String] =
{
  var Except_Validation = "Failed"
  // Except validation
  //println("Doing except validation between the tables")
  val gen1_sorted_columns = gen1DF.columns.sorted
  val gen2_sorted_columns = gen2DF.columns.sorted

  val gen1_sorted_df = gen1DF.select(gen1_sorted_columns.map(col): _*)
  val gen2_sorted_df = gen2DF.select(gen2_sorted_columns.map(col): _*)

  val Gen1_ex_cnt= gen1_sorted_df.drop(drop_col:_*).exceptAll(gen2_sorted_df.drop(drop_col:_*)).count()
  val Gen2_ex_cnt= gen2_sorted_df.drop(drop_col:_*).exceptAll(gen1_sorted_df.drop(drop_col:_*)).count()
  val ex_cnt = Gen1_ex_cnt + Gen2_ex_cnt
  
  if (ex_cnt == 0) 
  {
    //println("Except validation passed for table")
    Except_Validation = "Passed"
  }
  print(s"dataValidation=$Except_Validation")
  return Map("isPassed" ->Except_Validation,"Gen1_ex_cnt"-> (Gen1_ex_cnt).toString,"Gen2_ex_cnt" -> (Gen2_ex_cnt).toString)
}

def getDependentJobNames(tablePath: String): java.util.List[String]=
{
  
  return spark.sql(s"describe history delta.`$tablePath`").
                where("job.jobName is not null and job.jobName not in('Cloud Migration','Optimize_nzdata','my spark task')") 
                .select("job.jobName").distinct.map(r => r.getString(0)).collectAsList
  
}

def writeToTable(env:String, dag_id:String, gen1_path:String, gen2_path:String, 
                 gen1_cnt:String,gen2_cnt:String,gen1_dis_cnt:String,gen2_dis_cnt:String,
                 Gen1_ex_cnt:String,Gen2_ex_cnt:String,fileValRes:String,
                 count_validation:String,dis_count_validation:String,
                 except_validation:String,schemaValRes:String, errMsg:String, dependentJobNames: String):Unit=
{
   val fnl_tbl=Seq((env,dag_id, gen1_path, gen2_path, gen1_cnt, 
                    gen2_cnt, gen1_dis_cnt, gen2_dis_cnt, Gen1_ex_cnt, 
                    Gen2_ex_cnt, fileValRes, count_validation, 
                    dis_count_validation, except_validation, schemaValRes ,errMsg, dependentJobNames)).
                toDF("env","dag_id", "gen1_path", "gen2_path", "gen1_count", 
                     "gen2_count", "gen1_distinct_count", "gen2_distinct_count", 
                     "gen1_except_gen2", "gen2_except_gen1", "files_validation", 
                     "count_validation", "dis_count_validation", "except_validation", 
                     "schema_validation","error_msg","source_job")
        //fnl_tbl.show()
        val adlsPath = s"/dbfs/FileStorage/validation_final"
        val fnl_db = s"default"
        val rcn_table = s"validation_final"
        fnl_tbl.withColumn("ts",current_timestamp()).write.format("delta").option("mergeSchema", "true").mode("append").option("path",adlsPath).saveAsTable(s"$fnl_db.$rcn_table")
}

// COMMAND ----------

import util.control.Breaks._
import org.apache.spark.sql.DataFrame
def validation(tableList: String): Boolean =
{
  for(table <- tableList.split("\n"))
  {
    if(table.contains(","))
    {
    breakable
    {
        //print(s"table:$table")
        var errMsg = ""
        val table_path = table.split(",")
        val env = table_path(0)
        val dag_id = table_path(1)
        val gen1_path = table_path(2)
        val gen2_path = table_path(3)
        //val partCol = "load_date"//table_path(4)
      
        val tableName = gen1_path.split("/").last
      //   println(s"\n---Validating started for dag :$dag_id, table:$tableName---\n")
        //val fileValRes = fileValidation(gen1_path,gen2_path)
        var gen1DF:DataFrame = null
        var gen2DF:DataFrame = null
        var prod_gen1DF:DataFrame  = null
        val format = if (DeltaTable.isDeltaTable(spark, gen1_path)) "delta" else "parquet"
        try{
               if(gen2_path.contains("datanpii")){
                  //  print("scope:npii=>")
                   setGen1NpiiScope()
                   setGen2NpiiScope()
               }else{
                  //   print("scope:pii=>")
                    setGen1PiiScope()
                    setGen2PiiScope()
               }
               
               gen1DF = spark.read.format(format).load(gen1_path)
               if(env == "dev")
               {
                  println("reading:"+gen1_path.replaceAll("_test/priority2/[0-9][0-9][0-9]", ""))
                  prod_gen1DF = spark.read.format(format).load(gen1_path.replaceAll("_test/priority2/[0-9][0-9][0-9]", ""))
               }
               else
               {
                  //println("reading:"+gen1_path.replace("_test",""))
                  prod_gen1DF = spark.read.format(format).load(gen1_path.replace("_test",""))
               }
               gen2DF = spark.read.format(format).load(gen2_path)  
               val partCol = spark.sql(s"describe detail delta.`$gen1_path`")
                            .selectExpr("partitionColumns[0]").first.getString(0)
              //if its partitioned table we need to do the count validation on latest partition
               if(partCol!=null && partCol.trim().length > 0)
              {
                 gen1DF.createOrReplaceTempView("G1")
                 gen2DF.createOrReplaceTempView("G2")
                 //print("validating latest partition")
                 gen1DF = spark.sql(s"select * from G1 where $partCol=(select max($partCol) from G1 )")
                 gen2DF = spark.sql(s"select * from G2 where $partCol=(select max($partCol) from G2 )")
               }
       
        val fileValRes = "Passed"
        print(s"\n${dag_id} => ${gen1_path} => ")
        val schemaValRes = schemaValiadation(gen1DF,prod_gen1DF)
        val countValRes = countValiadation(gen1DF,gen2DF)
        val countDistValRes = validateDistinctCount(gen1DF,gen2DF, Seq("load_date","load_timestamp"))
        val exceptValdRes = validateData(gen1DF,gen2DF,Seq("load_date","load_timestamp","update_time","load_dtm","update_dtm","azure_export_date","q_promo_deal_id","last_update","ImportDate"))
        var dependentJobNames = "NA"
        if(format.equals("delta") && exceptValdRes("isPassed").equals("Failed")){
            dependentJobNames = getDependentJobNames(gen2_path).toString
        }
        writeToTable(env,dag_id, gen1_path, gen2_path, countValRes("gen1_cnt"), 
                    countValRes("gen2_cnt"),countDistValRes("gen1_dis_cnt"), 
                    countDistValRes("gen2_dis_cnt"), exceptValdRes("Gen1_ex_cnt"), 
                    exceptValdRes("Gen2_ex_cnt"),fileValRes, 
                    countValRes("isPassed"), countDistValRes("isPassed"), 
                    exceptValdRes("isPassed"), schemaValRes("isPassed"),schemaValRes("map"), dependentJobNames)
      //  print(s"\n---Validating completed for dag :$dag_id, table:$tableName---\n")
       }catch{
          case e: Exception =>{
            println(s"ERROR:files not present in the path for dag :$dag_id,table:$tableName,ERROR: "+e.getMessage())
            writeToTable(env,dag_id, gen1_path, gen2_path, 
                         "0", "0", "0","0", "-1", "-1", "Failed",
                         "Failed","Failed","Failed","Failed",e.getMessage(),"NA")
             break
          }
        }
    }
   }
  }
      return true
}

