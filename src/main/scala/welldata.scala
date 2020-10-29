import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructType}

import scala.io.Source

object welldata {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple CDE Run Example")
      .master("local")
      .getOrCreate()

    import spark.implicits._


    val schema = new StructType()
      .add("ReportDate",StringType,true)
      .add("API_WELLNO",LongType,true)
      .add("FileNo",IntegerType,true)
      .add("Company",StringType,true)
      .add("WellName",StringType,true)
      .add("Quarter",StringType,true)
      .add("Section",IntegerType,true)
      .add("Township",IntegerType,true)
      .add("Range",IntegerType,true)
      .add("County",StringType,true)
      .add("FieldName",StringType,true)
      .add("Pool",StringType,true)
      .add("Oil",StringType,true)
      .add("Wtr",StringType,true)
      .add("Days",StringType,true)
      .add("Runs",IntegerType,true)
      .add("Gas",StringType,true)
      .add("GasSold",IntegerType,true)
      .add("Flared",StringType,true)
      .add("Lat",LongType,true)
      .add("Long",LongType,true)

    val imputmessage = args(0)

    val df = spark.read.format("csv")
      .option("header","true")
      .schema(schema)
      .load("file:///Users/sunile.manjee/Downloads/2015_05.csv")

    df.printSchema()

    df.groupBy("WellName").count().show()


    println("\n*******************************")
    println("\n*******************************")
    println("source s3 file: "+imputmessage)
    println("\n*******************************")
    println("\n*******************************")



  }
}