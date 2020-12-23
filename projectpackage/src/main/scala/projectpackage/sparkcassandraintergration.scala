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
import org.apache.spark.sql.cassandra._ 
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrameReader
import com.datastax.spark.connector.cql.{CassandraConnector, _}
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
class sparkcassandraintergration {
   def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
     val spark = SparkSession.builder().appName("HBASE-DYNAMIC")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("spark.cassandra.connection.host","localhost")
        .config("spark.cassandra.connection.port","9042")
      .enableHiveSupport().master("local[*]").getOrCreate()		
      
      
val df=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","zeyobron").option("table","emp_date").load()
df.show()
      
      
      
   }
}