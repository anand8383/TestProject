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
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.hive._
import scala.io.Source
import com.mysql.jdbc.Driver
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.cassandra._ 
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.log4j.Level
//import org.scalatest.concurrent.Eventually
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scala.concurrent.Future
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.cassandra.{AnalyzedPredicates, CassandraPredicateRules, CassandraSourceRelation, TableRef}
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql.DataFrameReader
object sparkIntegrationCassandra1 {
case class cashschemaRDD(txnno:String,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String)
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
        .config("fs.s3a.access.key","AKIAJW7HFMLZ3JTSODAA")
        .config("fs.s3a.secret.key","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")
      .enableHiveSupport().master("local[*]").getOrCreate()	     
      
/*val df=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","zeyobron").option("table","emp_date").load()
df.show()      
      df.write.format("org.apache.spark.sql.cassandra").option("keyspace","zeyobron").option("table","emp_date1").mode("append").save()

      println("written")*/
      /*sc.hadoopConfiguration.set("fs.s3n.awaAccesKeyId","AKIAAIG3ZJCMAAPPSD5MQ")
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey","jInc7Ac1jq2Nx97d1eL7Q6er/L1nn/2zw8rdkTLA")
      val sonnets = sc.textFile("s3N://nifidest/usdata/usdatawh.csv")
sonnets.foreach(println)*/
 /*val data = "file:///C:///Users//anand//Desktop//Myfolder//Bigdata//txns"
//creating RDD
val usrdd = sc.textFile(data)

//removing header and split the data using "," and assign case class
val schemaRDD = usrdd.map(x=>x.split(",")).map(x=>cashschemaRDD(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
//schemaRDD.foreach(println)
schemaRDD.saveToCassandra("zeyobron","txntable1")
println("written down")
//Read table from cassandra as RDD
val cassandra_read = sc.cassandraTable("zeyobron","txntable1")
print("Printing my cassandra data")
cassandra_read.foreach(println)
cassandra_read.saveToCassandra("zeyobron","txntable",SomeColumns("txnno","txndate","custno","amount","category","product","city","state","spendby")
print("written done in Cassandra")*/

 /* val url = "jdbc:oracle:thin:@//mydatabase.cmw3falnhysb.ap-south-1.rds.amazonaws.com:1521/ORCL"
    //jdbc:oracle:thin:@//mydatabase.cmw3falnhysb.ap-south-1.rds.amazonaws.com:1521/ORCL
    val prop2 = new java.util.Properties()
    prop2.setProperty("user","ousername")
    prop2.setProperty("password","opassword")
    prop2.setProperty("driver","oracle.jdbc.OracleDriver")
    val df = spark.read.jdbc(url,"WEBDATA",prop2)
   // df.printSchema()
    df.show()
    df.write.format("csv").save("s3a://nifidest/Ananddir/")
    //val cols = df.columns.map(x=>x.toUpperCase())
    //val ndf = df.toDF(cols:_*)
    //ndf.printSchema()
/*val qry =
      """
        |CREATE table cassandra_db.URLTABLE (nationality text,cell text,dob text,email text,city text,state text,street text,zip text,
        |md5 text,first text,last text,title text,password text,phone text,large text,medium text,thumbnail text,registered text,salt text,
        |sha1 text,sha256 text,username text,seed text)
        |""".stripMargin
    val ss = spark.sql(qry)
    ss.show()*/
    //ss.write.format("csv").save("s3a://nifidest/Ananddir/")

    
    //ndf.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","URLTABLE").option("keyspace","cassandra_db").save()
//print("written done")
   /* val df=	spark.read.csv("s3a://nifidest/txndest/country=IND/000000_0")
    df.show()
    df.write.format("csv").save("s3a://nifidest/txntarget1/ss")
    println("data written")*/
      
   // val dff = spark.read.csv("s3a://nifidest/txndest/country=IND/000000_0")
    //dff.write.format("csv").save("s3a://nifidest/Anand_dir/")
    //dff.show()
    
    dff.write.format("org.elasticsearch.spark.sql")
			.option("es.nodes.wan.only","true")
			.option("es.port","443")
			.option("es.nodes", "https://search-zeyoes-b2frd4mtcj4jmovh3nuwf2jehe.ap-south-1.es.amazonaws.com/")
			.mode("append").save("anandindex/Anand_table1")
			print("written down")*/

val df=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","zeyobron").option("table","emp_date").load()
df.show()
    /*val ssc = new StreamingContext(conf,Seconds(2))
			val kafkaParams = Map[String, Object]("bootstrap.servers" -> "localhost:9092","key.deserializer" -> classOf[StringDeserializer],
					"value.deserializer" -> classOf[StringDeserializer],"group.id" -> "example","auto.offset.reset" -> "latest",
					"enable.auto.commit" -> (false: java.lang.Boolean))
			val topics = Array("zeyotopic10")
			val stream = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String, String](topics, kafkaParams))
			val streamdata=		stream.map(record => (record.value))
			val s1=streamdata.filter(x => !x.contains("empty_data"))

			s1.foreachRDD(rdd=>
			if(!rdd.isEmpty())
			{
				val jsondf=  spark.read.json(rdd)
						//val userwithoutnum= jsondf.withColumn("results",explode($"results")).select("results.user.username").withColumn("username",regexp_replace(col("username"),  "([0-9])", ""))

						val userwithoutnum= jsondf.withColumn("name",regexp_replace(col("name"),  "([0-9])", ""))

						userwithoutnum.show()
						userwithoutnum.write.format("org.elasticsearch.spark.sql")
						.option("es.nodes.wan.only","true")
						.option("es.port","443")
						.option("es.nodes", "https://search-sparkes-34nfk5qzsenrrst23pli65qalm.us-east-1.es.amazonaws.com")
						.mode("append")
						.save("userindex/usertable")

						println("done")

			})

			ssc.start()
			ssc.awaitTermination()*/


   }
}