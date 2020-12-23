package newpractise
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.databricks.spark.avro
import com.datastax.spark.connector._
import com.datastax.driver.core._
import com.datastax.driver.mapping._
import org.apache.spark.sql.hive._
import com.datastax.spark.connector._
import org.apache.spark.sql.functions.current_timestamp   
import scala.io.Source
import com.mysql.jdbc.Driver
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.cassandra._ 
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import com.datastax.spark.connector.streaming._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrameReader
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector}
import com.datastax.spark.connector.rdd.{EmptyCassandraRDD, ValidRDDType}
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.writer.WritableToCassandra
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
object oracledata {
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
      
      import spark.implicits._
			CassandraConnector(conf).withSessionDo { session =>
  session.execute("CREATE KEYSPACE test2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
  session.execute("CREATE TABLE test2.words (word text PRIMARY KEY, count int)")
}
   	
   		 val ohost = "jdbc:oracle:thin:@//myoracledb.conadtfguis7.ap-south-1.rds.amazonaws.com:1521/ORCL"
  val oprop = new java.util.Properties()
  oprop.setProperty("user", "ousername")
  oprop.setProperty("password", "opassword")
  oprop.setProperty("driver", "oracle.jdbc.OracleDriver")
  
     val qry = "(select table_name from all_tables where tablespace_name='USERS') tmp"
    val df1 = spark.read.jdbc(ohost, qry,oprop)
    val alltab = df1.select($"table_name").rdd.map(x=>x(0)).collect.toList
    //val alltab = Array("EMP")
    alltab.foreach { x =>
      val tab = x.toString
      println(s"Importing data from $tab table")
      val df = spark.read.jdbc(ohost, s"$x", oprop)
      df.show()
     // df.write.mode(SaveMode.Append).jdbc(mhost,s"$x",mprop) 
      }
   		
   		
   }  
}