package Projectcreationhandson
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
object Aug16Task2 {
	def main(args:Array[String]):Unit={
			//case class cashschemaRDD(txnno:Int,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String)
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					//val spark = SparkSessions.builders().master("local[*]").
					val spark = SparkSession.builder().master("local[*]").appName("SparkByExamples.com").getOrCreate();
			import spark.implicits._
			val myrdd = sc.textFile("file:///C:///Users//anand//Desktop//Myfolder//Bigdata//txns")
			val cashdata = myrdd.filter(x=>x.contains("cash"))
			//cashdata.foreach(println)
			val rowclassrdd = cashdata.map(x=>x.split(",")).map(x=>Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
			//rowclassrdd.foreach(println)
			//val schemadata = myrddcashdata.map(x=>x.split(",")).map(x=>cashschemaRDD(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
			val filter_data = rowclassrdd.filter(x=>x(4)=="Gymnastics" | x(4)=="Team Sports" | x(4) =="Exercise & fitness")
			//if you want to filter Row rdd column use below
			//val filter_row = rdd.filter(x=>x(0).toString().toInt<10)
}}
