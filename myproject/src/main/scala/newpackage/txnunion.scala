package Projectcreationhandson
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
object txnsUnionDataframe {
  case class myRddclass(txnno:String,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String)
  val struct_schema =
			  StructType(
			      StructField("txnno",StringType,true)::
			      StructField("txndate",StringType,true)::
			      StructField("custno",StringType,true)::
			      StructField("amount",StringType,true)::
			      StructField("category",StringType,true)::
			      StructField("product",StringType,true)::
			      StructField("city",StringType,true)::
			      StructField("state",StringType,true)::
			      StructField("spendby",StringType,true)::Nil)
			        
			      
  def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					//val spark = SparkSessions.builders().master("local[*]").
					val spark = SparkSession.builder().master("local[*]").appName("SparkByExamples.com").getOrCreate();
			import spark.implicits._
			//creating RDD and assign case class and convert into Dateframe
			/*val data = "file:///C:///Users//anand//Desktop//Myfolder//Bigdata//txns"
			val mydata = sc.textFile(data)
			val header = mydata.first()
			//•	Read txns using sc.textFile()
			val myRDD = mydata.filter(_!=header).map(_.split(",")).map(x=>myRddclass(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8))).toDF()
			myRDD.createOrReplaceTempView("table1")
			//•	DF2=create a struct convert into DF
			//create RDD and assign RowRDD and impose struct type schema and convert into dataframe.
			val myrowRDD = mydata.filter(_!=header).map(_.split(",")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
			val dfRDD = spark.createDataFrame(myrowRDD,struct_schema)
			dfRDD.createOrReplaceTempView("table2")
			//•	DF3=seamless read with same struct type
			val seamlessRDD = spark.read.format("csv").schema(struct_schema).option("header","true").option("inferSchema","true").load(data)
			seamlessRDD.createOrReplaceTempView("table3")
			
			val unionallthreetable = spark.sql ("select * from table1 union select * from table2 union select * from table3")
			//unionallthreetable.show()
			//syntax of save the dataframe data in local 
			unionallthreetable.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv("file:///C:///Users//anand//Desktop//Myfolder//Bigdata//unionoutput1")
			//just printing schema    
			unionallthreetable.printSchema()*/
  
			
			
		/*val data = spark.read.format("csv").option("Delimiter","~").option("Header","true").load("file:///C://Users//anand//Desktop//Myfolder//Bigdata//datasets//PractitionerLatest.txt")
data.show()*/
			
			
			
			
  }		
  
}