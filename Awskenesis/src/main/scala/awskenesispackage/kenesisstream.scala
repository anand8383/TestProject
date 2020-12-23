package awskenesispackage
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//import com.databricks.spark.avro
//import com.datastax.spark.connector._
//import com.datastax.driver.core._
//import com.datastax.driver.mapping._
import org.apache.spark.sql.hive._
//import com.datastax.spark.connector._
import org.apache.spark.sql.functions.current_timestamp   
import scala.io.Source
//#import com.mysql.jdbc.Driver
import org.apache.spark.sql.expressions.Window
//#import org.apache.spark.sql.cassandra._ 
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.{SQLContext, _}
//#import org.apache.spark.sql.execution.datasources.hbase._
//#import com.datastax.spark.connector.streaming._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrameReader
/*import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector}
import com.datastax.spark.connector.rdd.{EmptyCassandraRDD, ValidRDDType}
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.writer.WritableToCassandra
import com.datastax.spark.connector.cql.CassandraConnector*/
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.streaming.Duration
import org.apache.spark._
import org.apache.spark.sql._
import com.amazonaws.protocol.StructuredPojo
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.dynamodbv2.model.BillingMode
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import org.apache.hadoop.fs.s3a.S3AFileSystem
import com.fasterxml.jackson.dataformat.cbor.CBORFactory
import com.fasterxml.jackson.core.TSFBuilder
import org.apache.spark.streaming.kinesis.KinesisUtils

object kenesisstream {
  /* def main(args: Array[String]): Unit = {
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
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
   import spark.implicits._*/
 def b2s(a: Array[Byte]): String = new String(a)
  
  
def main(args:Array[String]):Unit={
val conf = new SparkConf().setAppName("ES").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
.set("spark.cassandra.connection.host", "localhost")
.set("spark.cassandra.connection.port", "9042")
val sc = new SparkContext(conf)
sc.setLogLevel("Error")
val spark = SparkSession
.builder()
.config("spark.cassandra.connection.host", "localhost")
.config("spark.cassandra.connection.port", "9042")
.getOrCreate()
import spark.implicits._

val ssc = new StreamingContext(conf,Seconds(2))

val kinesisStream111= KinesisUtils.createStream(
ssc, "anand_db","anand", "https://kinesis.ap-south-1.amazonaws.com",
"ap-south-1",InitialPositionInStream.TRIM_HORIZON, Seconds(1), StorageLevel.MEMORY_AND_DISK_2)

val finalstream=kinesisStream111.map(x=>b2s(x))

finalstream.print


ssc.start()
ssc.awaitTermination()


}
}
