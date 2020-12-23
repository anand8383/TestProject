package newpackage
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
   		
print(spark.version)
//step 1 create a path in the edge notes:
val local_file = "file:///C://Users//anand//Desktop//Myfolder//Bigdata//datasets//Alias_Names.txt"
//step 2 Read the path from Edge nodes
val readDf = sc.textFile(local_file)
// step 3 Reading the RDD data
val formatDF = readDf.collect().toList
formatDF.foreach(print)
   		
   		
   		
   		
   }
}