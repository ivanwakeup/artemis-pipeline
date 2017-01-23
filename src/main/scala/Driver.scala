/**
  * Created by developer on 1/16/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StructField, StructType}
import transform.ColumnTransformations
import transform.ColumnTransformations._
import formats.FixedWidthFile

object Driver {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName("Test")
      .getOrCreate()

    val thingsDf = session.read
      .option("inferSchema", "true")
      .option("sep", "|")
      .csv("/home/developer/projects/spark/source_thing_file.txt")

    //this array should be created dynamically when we assign column mappings in leto
    val tgtHeaders = Seq("source_id", "description", "class_code", "date")
    var things = thingsDf.toDF(tgtHeaders: _*)
    val headers = things.first()
    things = things.filter(row => row != headers)



    //chain transformations as dataframe
    val next = things
      .withColumn("desc_new", titleCase(col("description")))
      .drop("description")
      .withColumnRenamed("desc_new", "description")
      .withColumn("cc_new", stripCommas(col("class_code")))
      .drop("class_code")
      .withColumnRenamed("cc_new", "class_code")
      .withColumn("date_new", strToArtemisDate("m-d-yyyy")(col("date")))
      .drop("date")
      .withColumnRenamed("date_new", "date")

    next.show()

 /*   val personFile = new FixedWidthFile("/home/developer/projects/spark/source_person_file.txt", session, Array(10, 15, 10, 4), Array("id", "name", "birthday", "jobcode"))
      .toDataFrame

    personFile.show()

    val transform = personFile
      .withColumn("bday_new", pdToDash(col("birthday")))
      .withColumn("jobcode_new", pdStrip(col("jobcode")))
      .withColumnRenamed("id", "source_id")
      .drop("birthday")
      .drop("jobcode")
      .withColumnRenamed("bday_new", "birthday")
      .withColumnRenamed("jobcode_new", "jobcode")

    //val cols = Array(col("birthday"), col("jobcode"))
    //println(cols(0))

    val jobref = List(("100", "Beamer"))
    val ref = session.createDataFrame(jobref).toDF("jobcode", "jobname")

    //weird way to do a LEFT JOIN, but ok
    val joined = transform.join(ref, Seq("jobcode"), "left_outer")

    joined.show()*/
  }
}
