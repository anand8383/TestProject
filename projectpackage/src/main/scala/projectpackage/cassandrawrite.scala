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

object cassandrawrite {
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
      session.execute("DROP KEYSPACE IF EXISTS testkeyspace")
      session.execute("CREATE KEYSPACE testkeyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
      session.execute("USE testkeyspace")
      session.execute("CREATE TABLE emp(emp_id int PRIMARY KEY,emp_name text,emp_city text,emp_sal varint,emp_phone varint)")
      session.execute("INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(1,'John', 'London', 0786022338, 65000);")
      session.execute("INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(2,'David', 'Hanoi', 0986022576, 40000);")
      session.execute("INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(3,'John Cass', 'Scotland', 0786022342, 75000);")
      session.execute("INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(4,'Bob Cass', 'Bristol', 0786022258, 80950);")
    }
    )

    //Read Cassandra data using DataFrame
    val df = spark.read
                  .format("org.apache.spark.sql.cassandra")
                  .options(Map( "table" -> "emp", "keyspace" -> "testkeyspace"))
                  .load()

    //Display all row of the emp table
    println("Details of all employees: ")
    df.show()

    //Use Selection and Filtering to find all employees who have high salary (>50000)
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
    df_highSal.show()
    
    highSal.createOrReplaceTempView("t2")
    df.createOrReplaceTempView("t1")
    val ss = spark.sql("select t1.emp_id,t1.emp_city,t1.emp_name,t1.emp_phone,t1.emp_sal,t2.emp_city from t1 join t2 on t1.emp_name=t2.emp_name  ")

    val ss1 = df.join(highSal,$"emp_name"===$"emp_name".alias("empname"),"left_outer")
    print("ss resultset")
    ss.show()
    print("ss1 resultset")
    ss1.show()

      
      
      
  }
}