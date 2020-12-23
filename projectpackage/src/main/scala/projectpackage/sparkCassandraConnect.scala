package projectpackage
import org.apache.spark.SparkConf;
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
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.{EmptyCassandraRDD, ValidRDDType}
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.writer.WritableToCassandra
import org.apache.spark.streaming.twitter._

import java.util.Date
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
object sparkCassandraConnect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    //val sc = new SparkContext(conf)
    //sc.setLogLevel("Error")
     val spark = SparkSession.builder().appName("spark_cassandra integration")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("spark.cassandra.connection.host","localhost")
        .config("spark.cassandra.connection.port","9042")
      .enableHiveSupport().master("local[*]").getOrCreate()	 
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
      
      //Implicit methods available in Scala for converting common Scala objects into DataFrames
    import spark.implicits._

    //Get Spark Context from Spark session
    val sparkContext = spark.sparkContext
 val APIkey="Vpl3wEw078ctX8fEyrxWToepS"
    val APIsecretkey="tDU1i0kqMtieR7veb41UkstFWG0GSVPDzKAuqrTvaOgO1L8ycU"
    val Accesstoken="181460431-IPAq9EynnA4p7nZ84LhXNgXWlqCNX017V2bn3jUT"
    val Accesstokensecret ="DR6iuNIHLVmCZgggtMyKcAR4201uIyl8SDXO4Gy1CLHyB"
    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)
    val filters="covid, covid19, corona, second wave"
    val stream = TwitterUtils.createStream(ssc, None, Seq(filters.toString()))
    stream.foreachRDD { x=>
        val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        
       /*  val myurl = "jdbc:oracle:thin:@//myoracledb.conadtfguis7.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop =new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df =spark.read.jdbc(myurl,"emp",oprop)
    df.show()

      /*val df = x.map(x=>(x.getText(),x.getUser.getScreenName())).toDF("msg","user")
        df.createOrReplaceTempView("tab")
        val del = spark.sql("select * from tab where city='del'")
        val blr =  spark.sql("select * from tab where city='blr'")
        val mas =  spark.sql("select * from tab where city='mas'")
        val other =  spark.sql("select * from tab where city not in ('mas','blr','del')")
        del.write.mode(SaveMode.Append).jdbc(ourl,"delhilive",oprop)
             blr.write.mode(SaveMode.Append).jdbc(ourl,"blrlive",oprop)
             mas.write.mode(SaveMode.Append).jdbc(ourl,"maslive",oprop)*/
        df.write.mode(SaveMode.Overwrite).jdbc(myurl,"otherlive",oprop)
        df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map( "table" -> "tab1", "keyspace" -> "testkeyspace")).save()
        df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map( "table" -> "tab2", "keyspace" -> "testkeyspace")).save()
        df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map( "table" -> "tab3", "keyspace" -> "testkeyspace")).save()
        df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map( "table" -> "tab4", "keyspace" -> "testkeyspace")).save()
       df.show(false)*/


    }


  }
}
