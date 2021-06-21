package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object GitObj {
  
  def main(args:Array[String]):Unit={


		val conf=new SparkConf().setAppName("spark_integration").setMaster("local[*]")
				val sc=new SparkContext(conf)
				sc.setLogLevel("Error")
				val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
				import spark.implicits._
  
  val jsondf=spark.read.format("json").option("multiline","true").load("file:///c://data//complexjson//gender.json")
  jsondf.show()
  jsondf.printSchema()
  
  
  
  }
}