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
import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector}
import com.datastax.spark.connector.rdd.{EmptyCassandraRDD, ValidRDDType}
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.writer.WritableToCassandra


import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
object hitURLdataToCassandra {
 
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
     val spark = SparkSession.builder().appName("HBASE-DYNAMIC")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("spark.cassandra.connection.host","localhost")
      .config("spark.cassandra.output.ifNotExists","true")
        .config("spark.cassandra.connection.port","9042")
      .enableHiveSupport().master("local[*]").getOrCreate()
      
/*val result = scala.io.Source.fromURL("https://randomuser.me/api/0.8/?results=100").mkString
//Getting  data as string()
val jsonResponseOneLine = result.toString().stripLineEnd
//convert string into RDD
val jsonRdd = sc.parallelize(jsonResponseOneLine :: Nil)
System.setProperty("http.agent", "Chrome")
//Convert RDD to Dataframe
val jsonDf = spark.read.format("json").option("multiLine","true").json(jsonRdd)
jsonDf.printSchema()
val formatjson = jsonDf.withColumn("RootArray",explode(col("results"))).select("nationality","RootArray.user.cell","RootArray.user.dob",
"RootArray.user.email","RootArray.user.location.city","RootArray.user.location.state","RootArray.user.location.street","RootArray.user.location.zip",
"RootArray.user.md5","RootArray.user.name.first","RootArray.user.name.last","RootArray.user.name.title","RootArray.user.password","RootArray.user.phone","RootArray.user.picture.large",
"RootArray.user.picture.medium","RootArray.user.picture.thumbnail","RootArray.user.registered","RootArray.user.salt","RootArray.user.sha1","RootArray.user.sha256","RootArray.user.username","seed")
val dff=formatjson.withColumn("time_stamp", current_timestamp())
dff.show(10,false)
dff.printSchema()
  //newdataframe.write.format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host","localhost").option("spark.cassandra.connection.port","9042").option("keyspace","zeyobron").
  //option("confirm.truncate","true").mode("append").saveAsTable("cassandratable")
//dff.write.format("org.apache.spark.sql.cassandra").options("append").options(Map("keyspace"->"zeyobron","table"->"urldynamictable")).save()

//,SomeColumns("nationality","cell","dob","email","city","state","street","zip","md5","first","last","title","password","phone","large","medium","thumbnail","registered","salt","sha1","sha256","username","seed","time_stamp PRIMARY KEY"))
print("written done in Cassandra")*/
  //Implicit methods available in Scala for converting common Scala objects into DataFrames
    import spark.implicits._
   //Get Spark Context from Spark session
    val sparkContext = spark.sparkContext

    //Set the Log file level
    sparkContext.setLogLevel("WARN")

    //Connect Spark to Cassandra and execute CQL statements from Spark applications
    val connector = CassandraConnector(sparkContext.getConf)
    connector.withSessionDo(session =>
    {
      session.execute("DROP KEYSPACE IF EXISTS testkeyspace1")
      session.execute("CREATE KEYSPACE testkeyspace1 WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
      session.execute("USE testkeyspace")
      session.execute("CREATE TABLE emp1(emp_id int PRIMARY KEY,emp_name text,emp_city text,emp_sal varint,emp_phone varint)")
      session.execute("INSERT INTO emp1 (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(1,'John', 'London', 0786022338, 65000);")
      session.execute("INSERT INTO emp1 (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(2,'David', 'Hanoi', 0986022576, 40000);")
      session.execute("INSERT INTO emp1 (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(3,'John Cass', 'Scotland', 0786022342, 75000);")
      session.execute("INSERT INTO emp1 (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(4,'Bob Cass', 'Bristol', 0786022258, 80950);")
    }
    )

    //Read Cassandra data using DataFrame
    val df = spark.read
                  .format("org.apache.spark.sql.cassandra")
                  .options(Map( "table" -> "emp1", "keyspace" -> "testkeyspace1"))
                  .load()

    //Display all row of the emp table
    println("Details of all employees: ")
    df.show()
    println("fetching table from oracl")
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
    }

   /* //Use Selection and Filtering to find all employees who have high salary (>50000)
    //(In spark, Where is an alias for Filter)
    val highSal=df.select("emp_name","emp_city").filter($"emp_sal">50000)

   //Create a Cassandra Table from a Dataset
    highSal.createCassandraTable("testkeyspace","highsalary")

    //Using a format helper to save data into a Cassandra table
    highSal.write.cassandraFormat("highsalary","testkeyspace").save()

    //Read Cassandra data using DataFrame
    val df_highSal = spark.read.cassandraFormat("highsalary", "testkeyspace").load()

    //Display all high salary employees
    println("All high salary employees: ")
    df_highSal.show()*/
  
  
  
  
  
  }
}