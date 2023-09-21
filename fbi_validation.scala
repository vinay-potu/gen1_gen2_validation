// Databricks notebook source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructField}
import io.delta.tables._
import org.apache.spark.sql.functions._
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

def fileValidation(gen1_path:String,gen2_path:String):String={
  var res = "Failed"
  try{
  res = if(dbutils.fs.ls(gen1_path).map(_.path).filter(_.contains("parquet")).length >0 && dbutils.fs.ls(gen2_path).map(_.path).filter(_.contains("parquet")).length >0) "Passed" else "Failed"
  }catch {
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
  val Gen1_ex_cnt= gen1DF.drop(drop_col:_*).exceptAll(gen2DF.drop(drop_col:_*)).count()
  val Gen2_ex_cnt= gen2DF.drop(drop_col:_*).exceptAll(gen1DF.drop(drop_col:_*)).count()
  val ex_cnt = Gen1_ex_cnt + Gen2_ex_cnt
  
  if (ex_cnt == 0) 
  {
    //println("Except validation passed for table")
    Except_Validation = "Passed"
  }
  print(s"dataValidation=$Except_Validation =>")
  return Map("isPassed" ->Except_Validation,"Gen1_ex_cnt"-> (Gen1_ex_cnt).toString,"Gen2_ex_cnt" -> (Gen2_ex_cnt).toString)
}

case class Validation(
env:String, 
dag_id:String, 
gen1_path:String, 
gen2_path:String, 
gen1_hist_path:String, 
gen2_hist_path:String,
gen1_cnt:String,
gen2_cnt:String,
gen1_dis_cnt:String,
gen2_dis_cnt:String,
Gen1_ex_cnt:String,
Gen2_ex_cnt:String,
fileValRes:String,
count_validation:String,
dis_count_validation:String,
except_validation:String,
schemaValRes:String, 
errMsg:String,

gen1_hist_cnt:String,
gen2_hist_cnt:String,
gen1_dis_hist_cnt:String,
gen2_dis_hist_cnt:String,
Gen1_ex_hist_cnt:String,
Gen2_ex_hist_cnt:String,
fileValHistRes:String,
count_hist_validation:String,
dis_count_hist_validation:String,
except_hist_validation:String,
schemaValHistRes:String, 
errMsgHist:String
)

def writeToTable(v:Validation):Unit=
{
  val fnl_tbl = Seq(v).toDF
  //fnl_tbl.show()
  val adlsPath = s"/dbfs/FileStorage/ingestion_validation"
  val fnl_db = s"default"
  val rcn_table = s"ingestion_validation"
  fnl_tbl.withColumn("ts",current_timestamp()).write.format("delta").option("mergeSchema", "true").mode("append").option("path",adlsPath).saveAsTable(s"$fnl_db.$rcn_table")
}

def computePath(x_path: String): String={
  val sdf=new java.text.SimpleDateFormat("yyyy-MM-dd")
  val files = dbutils.fs.ls(x_path)
  if(! (files.count(x => x.path.contains("=")) > 0)){
    return x_path
  }else{
    return files.filter(x => x.path.contains("=")).sortBy(x=>sdf.parse(x.path.split("=")(1).split("/")(0)).getTime).reverse.head.path
  }
}


// COMMAND ----------

import util.control.Breaks._
import org.apache.spark.sql.DataFrame



def validation(tableList:String): Boolean =
{
  val sdf=new java.text.SimpleDateFormat("yyyy-MM-dd")
  for(table <- tableList.split("\n") )
  {
    if(table.contains(","))
    {
    breakable
    {
        //print(s"table:$table")
        var errMsg = ""
        var table_path = table.split(",")
        val env = table_path(0)
        val dag_id = table_path(1)
        var gen1_path = table_path(2)
        var gen2_path = table_path(3)
        var gen1_hist_path = table_path(4)
        var gen2_hist_path = table_path(5)
        var prod_gen1_path = gen1_path.replace("_test","")
        var prod_gen1_hist_path = gen1_hist_path.replace("_test","")
        //val partCol = "load_date"//table_path(4)
      
        val tableName = gen1_path.split("/").last
        print(s"\n dag :$dag_id, table:$tableName===> ")
        //val fileValRes = fileValidation(gen1_path,gen2_path)
        var gen1DF:DataFrame = null
        var gen2DF:DataFrame = null
        var prod_gen1DF:DataFrame  = null
        var gen1HistDF:DataFrame = null
        var gen2HistDF:DataFrame = null
        var prod_gen1HistDF:DataFrame  = null
        try{
               if(gen2_path.contains("datanpii")){
                   //print("scope:npii=>")
                   setGen1NpiiScope()
                   setGen2NpiiScope()
               }else{
                    //print("scope:pii=>")
                    setGen1PiiScope()
                    setGen2PiiScope()
               }
               val format = if (DeltaTable.isDeltaTable(spark, gen1_path)) "delta" else "parquet"
               var partCol = ""
               var maxPartDt = ""
          
//                if(gen1_path.contains("=")){
                 prod_gen1_path = computePath(prod_gen1_path)
                 prod_gen1_hist_path = computePath(prod_gen1_hist_path)
          
                 gen1_path = computePath(gen1_path)
                 gen2_path = computePath(gen2_path)
                 gen1_hist_path = computePath(gen1_hist_path)
                 gen2_hist_path = computePath(gen2_hist_path)
                // for schema validation
                 
                 print(s"\ngen1_path=$gen1_path,\ngen2_path=$gen2_path,\ngen1_hist_path=$gen1_hist_path,\ngen2_hist_path=$gen2_hist_path\n" )
//                }
               
               gen1DF = spark.read.format(format).load(gen1_path)
               gen2DF = spark.read.format(format).load(gen2_path)  
               prod_gen1DF = spark.read.format(format).load(prod_gen1_path)
               gen1HistDF = spark.read.format(format).load(gen1_hist_path)
               prod_gen1HistDF = spark.read.format(format).load(prod_gen1_hist_path)
               gen2HistDF = spark.read.format(format).load(gen2_hist_path)  
        }catch{
          case e: Exception =>{
            println(s"ERROR:files not present in the path for dag :$dag_id,table:$tableName,ERROR: "+e.getMessage())
            writeToTable(new Validation(
                        env,
                        dag_id,
                        gen1_path,
                        gen2_path,
                        gen1_hist_path,
                        gen2_hist_path,
                        "0",
                        "0",
                        "0",
                        "0",
                        "-1",
                        "-1",
                        "Failed",
                        "Failed",
                        "Failed",
                        "Failed",
                        "Failed",
                        e.getMessage(),
                        "0",
                        "0",
                        "0",
                        "0",
                        "-1",
                        "-1",
                        "Failed",
                        "Failed",
                        "Failed",
                        "Failed",
                        "Failed",
                        ""
                        )
                        )
             break
          }
        }
        val fileValRes = "Passed"
        val fileValHistRes = "Passed"
        val schemaValRes = schemaValiadation(gen1DF,prod_gen1DF)
        val countValRes = countValiadation(gen1DF,gen2DF)
        val countDistValRes = validateDistinctCount(gen1DF,gen2DF, Seq("load_date","load_timestamp"))
        val exceptValdRes = validateData(gen1DF,gen2DF,Seq("load_date","load_timestamp"))
        println("\nHist validation : ")
        val schemaValHistRes = schemaValiadation(gen1HistDF,prod_gen1HistDF)
        val countValHistRes = countValiadation(gen1HistDF,gen2HistDF)
        val countDistValHistRes = validateDistinctCount(gen1HistDF,gen2HistDF, Seq("load_date","load_timestamp"))
        var exceptValdHistRes :Map[String,String] =null
        if(gen1_hist_path.contains("="))
              exceptValdHistRes = validateData(gen1HistDF,gen2HistDF,Seq("load_date","load_timestamp"))
       else
            exceptValdHistRes =Map("isPassed" ->"NA","Gen1_ex_cnt"-> "0","Gen2_ex_cnt" -> "0")
        writeToTable(
                    new Validation(
                     env,
                     dag_id, 
                     gen1_path, 
                     gen2_path, 
                     gen1_hist_path, 
                     gen2_hist_path, 
                     countValRes("gen1_cnt"), 
                     countValRes("gen2_cnt"),
                     countDistValRes("gen1_dis_cnt"),
                     countDistValRes("gen2_dis_cnt"), 
                     exceptValdRes("Gen1_ex_cnt"),
                     exceptValdRes("Gen2_ex_cnt"),
                     fileValRes, 
                     countValRes("isPassed"), 
                     countDistValRes("isPassed"), 
                     exceptValdRes("isPassed"), 
                     schemaValRes("isPassed"), 
                     schemaValRes("map"),
                    
                     countValHistRes("gen1_cnt"), 
                     countValHistRes("gen2_cnt"),
                     countDistValHistRes("gen1_dis_cnt"),
                     countDistValHistRes("gen2_dis_cnt"), 
                     exceptValdHistRes("Gen1_ex_cnt"),
                     exceptValdHistRes("Gen2_ex_cnt"),
                     fileValHistRes, 
                     countValHistRes("isPassed"), 
                     countDistValHistRes("isPassed"), 
                     exceptValdHistRes("isPassed"), 
                     schemaValHistRes("isPassed"), 
                     schemaValHistRes("map")
                      )
                    )
       //print(s"\n---Validating completed for dag :$dag_id, table:$tableName---\n")
    }
   }
  }
      return true
}

// COMMAND ----------


