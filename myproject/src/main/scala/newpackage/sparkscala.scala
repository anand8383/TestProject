/*package newpackage
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.databricks.spark.avro
import org.apache.spark.sql.hive._
import scala.io.Source
import com.mysql.jdbc.Driver
import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.execution.datasources.hbase._

object sparkscala {
   def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
     val spark = SparkSession.builder().appName("HBASE-DYNAMIC")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport().master("local[*]").getOrCreate()			
			
      import spark.implicits._
			val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
			import hiveContext._	

     System.setProperty("http.agent", "Chrome")
    
    spark.conf.set("parquet.enable.summary-metadata", "false")
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    val lstval=spark.read.format("csv").load("file:///home/cloudera/lstval")
   lstval.createOrReplaceTempView("lst_temp")
   val ltval=spark.sql("select _c0 as lst from lst_temp").first().get(0)
   print("=====inc====="+ltval)
   val dbname = args(0)
   val tablename = args(1)
   val username = args(2)
   val pass = args(3)
   
   
    val rds_userdata=spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/dbname")
			.option("driver", "com.mysql.jdbc.Driver")
			.option("dbtable","tablename")
			//.option("query","
			.option("user","username")
			.option("password","pass")
			
			.load()
 rds_userdata.show()
 rds_userdata.createOrReplaceTempView("rds_users_data")
 val rds_df=spark.sql("select id,username,amount,ip,createdt,value,score,regioncode,status,method,key1,count,type,site,statuscode from rds_users_data where id>"+ltval+"")
 println("select id,username,amount,ip,createdt,value,score,regioncode,status,method,key1,count,type,site,statuscode from wblog where id>"+ltval+"")
 rds_df.show() 
 //->write - 1 (raw data from RDBMS to HDFS)
 rds_df.withColumn("current_date",current_date()).write.partitionBy("current_date")
.format("com.databricks.spark.avro").mode("append").save("hdfs:/user/cloudera/current_date/raw_dml_data")

//-->write - 4 (raw data from RDS to hbase)
val str_catalog = s"""{ 
		|"table":{"namespace":"DLM","name":"web_log_user_view_DLM"},
		|"rowkey":"rowkey",
		|"columns":{
		|"id":{"cf":"rowkey","col":"rowkey","type":"string"},
		|"username":{"cf":"rdscf","col":"username","type":"string"},
		|"amount":{"cf":"rdscf","col":"amount","type":"string"},
		|"ip":{"cf":"rdscf","col":"ip","type":"string"},
		|"createdt":{"cf":"rdscf","col":"createdt","type":"string"},
		|"value":{"cf":"rdscf","col":"value","type":"string"},
		|"score":{"cf":"rdscf","col":"score","type":"string"},
		|"regioncode":{"cf":"rdscf","col":"regioncode","type":"string"},
		|"status":{"cf":"rdscf","col":"status","type":"string"},
		|"method":{"cf":"rdscf","col":"method","type":"string"},
		|"key1":{"cf":"rdscf","col":"key1","type":"string"},
		|"count":{"cf":"rdscf","col":"count","type":"string"},
		|"type":{"cf":"rdscf","col":"type","type":"string"},
		|"site":{"cf":"rdscf","col":"site","type":"string"},
		|"statuscode":{"cf":"rdscf","col":"statuscode","type":"string"}
		}
|}""".stripMargin

//rds_df.write.options(Map(HBaseTableCatalog.tableCatalog -> str_catalog, HBaseTableCatalog.newTable -> "5"))
//.mode("append").format("org.apache.spark.sql.execution.datasources.hbase").save()
 
val maxid=spark.sql("select max(id) as maxidd from rds_users_data")
  val maxid1=maxid.first().get(0)
  val max=maxid1
  println("max id====>>"+max)
  maxid.write.format("csv").mode("overwrite").save("file:///home/cloudera/lstval")
    
  
  val userdata = scala.io.Source.fromURL("https://randomuser.me/api/0.8/?results=100").mkString
  System.setProperty("http.agent", "Chrome")
 
  val jsonResponseOneLine = userdata.toString().stripLineEnd   
  val json = sc.parallelize(userdata :: Nil)
    val json_file = spark.read.option("multiLine", "true").json(json)
    json_file.printSchema()
    
     val df2 = json_file.withColumn("results", explode(col("results")))
      .select("results.user.gender","results.user.name.title","results.user.name.first", // df.select($"_1".alias("x1"))
          "results.user.name.last","results.user.location.street",
          "results.user.location.city","results.user.location.state",
          "results.user.location.zip","results.user.email","results.user.username",
          "results.user.password", "results.user.salt", "results.user.md5",
          "results.user.sha1","results.user.sha256","results.user.registered",
          "results.user.dob","results.user.phone","results.user.cell","results.user.picture.large",
          "results.user.picture.medium","results.user.picture.thumbnail")
          .withColumn("user_name",regexp_replace(lower($"username"),"[^a-zA-Z]","")).dropDuplicates("username")
       df2.printSchema()
       //df2.show()
       println("df2======>>"+df2.count())
       val resultss = df2.drop("username")
       resultss.show()
  //}   
      val fiftyusernames=resultss.limit(50)  
      println("50 counts====>>"+fiftyusernames.count())
      fiftyusernames.show(false)
      //write 1: Web api data to HDFS 
      fiftyusernames.withColumn("current_date",current_date()).write.partitionBy("current_date")
      .format("com.databricks.spark.avro").mode("append").save("hdfs:/user/cloudera/current_date/raw_web_data")
      val final_left_join=rds_df.join(fiftyusernames,rds_df("username")===fiftyusernames("user_name"),"left")
  
     //val final_left_join=fiftyusernames.join(rds_df,fiftyusernames("user_name")===rds_df("username"),"left")
    final_left_join.show(false)
    // val finalresultset = final_left_join.dropDuplicates("user_name").toDF()
    val finaldf_without_nulls=final_left_join.na.fill("Not Available").na.fill(0)
    finaldf_without_nulls.show()
    println("count of rds data"+rds_df.count()+" "+"No of columns in rds data"+" "+rds_df.columns.size)
    println("count of fiftyusernames data"+fiftyusernames.count()+" "+"No of columns in fiftyusernames"+" "+ fiftyusernames.columns.size)
    println("count of final_left_join data"+final_left_join.count()+" "+"No of columns in final_left_join"+" "+final_left_join.columns.size)
    println("finalresultset============>>>"+df2.count())
    println("finaldf_without_nulls====>>"+finaldf_without_nulls.count())
    
    spark.sql("use webanalytics")
    finaldf_without_nulls.withColumn("createdt",expr("from_unixtime(unix_timestamp(createdt,'dd/MMM/yyyy'),'yyyy-MM-dd')"))
    .withColumn("year",year(col("createdt")))
    .withColumn("month",month(col("createdt")))
    .withColumn("day",dayofmonth(col("createdt")))
    //.write.format("hive").partitionBy("year","month","days").mode("overwrite").save("hdfs:/user/cloudera/weblog_rds_final")
    //.write.format("hive").partitionBy("year","month","day").mode("append").insertInto("weblog_user_web_dlm_analytics")
  /*Exception in thread "main" org.apache.spark.sql.AnalysisException: insertInto() 
   * can't be used together with partitionBy(). Partition columns have already been defined for the table. 
   * It is not necessary to use partitionBy().;
   */
    .write.format("hive").mode("Append").insertInto("weblog_user_web_dlm_analytics")
    finaldf_without_nulls.printSchema()
    println("ecplise code completed")
  }
}*/