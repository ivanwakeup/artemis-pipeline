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



    /*chain transformations as dataframe
    val next = things
      .withColumn("desc_new", titleUDF(col("description")))
      .drop("description")
      .withColumnRenamed("desc_new", "description")
      .withColumn("cc_new", stripCommasUDF(col("class_code")))
      .drop("class_code")
      .withColumnRenamed("cc_new", "class_code")
      .withColumn("date_new", formatStr(col("date")))
      .drop("date")
      .withColumnRenamed("date_new", "date")
*/
    def getRow(x: String, widths: Array[Int]): Row = {
      var cnt = 0
      var pos = 0
      val colArray = new Array[String](widths.length)
      for (width <- widths) {
        //println(x.length)
        colArray(cnt) = x
          .substring(pos, Math.min((pos + width), x.length))
          .trim
        pos = pos + width
        cnt += 1
      }
      //colArray.mkString("|")
      Row.fromSeq(colArray)
    }


    //kind of dumb but you NEED the schema defined to do the below mapping
    //seems ridiculous to define a schema and then map it to Dataset[Row] though. should find a better way to do this.

    val fixedSchema = "id,name,birthday,jobcode"
    val fields = fixedSchema.split(",")
      .map(field => StructField(field, StringType, nullable = true))
    val schema = StructType(fields)
    val encoder = RowEncoder(schema)

    val personFile = session.read.textFile("/home/developer/projects/spark/source_person_file.txt")
      .map(line  => getRow(line, Array(10, 15, 10, 4)))(encoder)
      .toDF()

/*
    val transform = personFile
      .withColumn("bday_new", pdToDash(col("birthday")))
      .withColumn("jobcode_new", pdStrip(col("jobcode")))
      .withColumnRenamed("id", "source_id")
      .drop("birthday")
      .drop("jobcode")
      .withColumnRenamed("bday_new", "birthday")
      .withColumnRenamed("jobcode_new", "jobcode")


    val jobref = List(("100", "Beamer"))
    val ref = session.createDataFrame(jobref).toDF("jobcode", "jobname")

    //weird way to do a LEFT JOIN, but ok
    val joined = transform.join(ref, Seq("jobcode"), "left_outer")

    joined.show()*/
  }
}
