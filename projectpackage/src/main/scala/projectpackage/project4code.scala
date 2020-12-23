package projectpackage

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
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrameReader
object project4code {
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
   		
print(spark.version)
//step 1 create a path in the edge notes:
//val local_file = "file:///C://Users//anand//Desktop//Myfolder//Bigdata//datasets//Alias_Names.txt"
val local_file = "file:///home/cloudera/pysparkProject4/Alias_Names.txt"
//step 2 Read the path from Edge nodes
val readDf = sc.textFile(local_file)
// step 3 Reading the RDD data
val formatDF = readDf.collect().toList
//formatDF.foreach(println)
//creating catalog header string 
val hbase_cataglog_first_string = "{\t\n\t \"table\":{\"namespace:\":\"default\",\"name\":\"hbase_tract10\"},\n\t \"rowkey\":\"masterid\",\n\t \"columns\":{ \n\t \"masterid\":{ \"cf\":\"rowkey\",\"col\":\"masterid\",\"type\":\"string\"},\n"

//hbase_cataglog_last_string.foreach(print)
//creating a variable to get column from hbase as var is mutable and val is immutable
//In simple terms: var = variable val = variable + final
var hbase_col =""
//creating a variable for spark from hbase_col
var spark_col= ""
//creating loop string for creating hbase catalog
var loop_string = " \t \""
for (row <- formatDF) {
  hbase_col = row.split(",")(0)
  spark_col = row.split(",")(1)
  loop_string = loop_string+spark_col.concat("\":{\"cf\":\"cf\",\"col\":\""+hbase_col+"\", \"type\":\"string\"},\n\t \"")
}
//remove the last chars
    val loop_string_remove_last_char = loop_string.slice(0, loop_string.length -2)
    //creating catalog trailer string 
hbase_cataglog_first_string.foreach(print)
val hbase_cataglog_last_string = "\n\t } \n\t } .stripMargin\n"
    //print(loop_string_remove_last_char)
//remove the end of comma
    val main_loop =loop_string_remove_last_char.substring(0,loop_string_remove_last_char.lastIndexOf(","));
//concat the first ,loop string and last string
    val final_string = hbase_cataglog_first_string.concat(main_loop).concat(hbase_cataglog_last_string)
    print(final_string)
 //create the method to read to read data from hbase table
  def withCatalog(cat: String): DataFrame = {
  spark.read.options(Map(HBaseTableCatalog.tableCatalog->cat)).format("org.apache.spark.sql.execution.datasources.hbase").load()
  
  }
  
    val mydf = withCatalog(final_string)
    
    mydf.show(10,false)
    //val dff = spark.read.fo
	print("Dynamic DataFrame has been create")
	//val df = spark.read.format("csv").option("header", true).load(local_file)
	//=As we still we prepared dynamic String and read the data from the HBASE Database
  // After readinhg the data from HBASE, We should create the Tables based on prefixs from the Alias_Names
	//for this, we should get the distinct prefixes columns from the hbase dataframe
	val distinct_column_names = mydf.columns.filter(x=>x!="masterid").map(y=>y.split("_")(0)).toList.distinct
	//create a loop here we creating hive table based on teh the distinct prefixes column and writing into hive database
	for(col_name<-distinct_column_names){
	  val table_names = "bigdata_project4."+col_name+"_tab"
	   val df1 =mydf.select(mydf.columns.filter(_.startsWith(col_name)).map(mydf(_)) : _ *)
	   df1.show()
	   df1.write.format("hive").mode("overwrite").saveAsTable(table_names)
	}
	
	print("execution done")
	

}
}
