package transform

import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql._
import scala.collection.immutable.StringOps

/**
  * Created by developer on 1/17/17.
  */
object ColumnTransformations {


  val titleCase = udf((str: String) => str.toLowerCase.capitalize)
  val stripCommas = udf((str: String) => str.replaceAll(",", ""))

  //define date input/output formats
  val formatStr = udf((str: String, format: String) => {
    val inputFormat = new SimpleDateFormat(format)
    val outputFormat = new SimpleDateFormat("mm-dd-yyyy")
    outputFormat.format(inputFormat.parse(str))
  })

  //functions with more than 1 argument have to be curried if you want to use them as a UDF, like so
  val strToArtemisDate = (format: String) => udf((str: String) => {
    val inputFormat = new SimpleDateFormat(format)
    val outputFormat = new SimpleDateFormat("mm-dd-yyyy")
    outputFormat.format(inputFormat.parse(str))
  })

  val pdToDash = udf((str: String) => {
    str.replaceAll("\\.", "-")
  })

  val pdStrip = udf((str: String) => {
    str.replaceAll("\\.", "")
  })

  val icd9Format = udf((str: String) => {
    if(str.trim.length == 5)
      str
    else str.padTo(5,"0").mkString
  })

  val cleanZip = udf((str: String) => {
    val regex = "^[0-9]{5}(?:-[0-9]{4})?$"
    if(str.matches(regex))
      str
    else str.trim.replaceAll("[^\\p{L}\\p{Nd}]+", "")
  })

  val cleanAddress = udf((str: String) => {
    val regex = "^[a-zA-Z0-9.-\\s]+$"
    if(str.matches(regex))
      str
    else str.replaceAll("[^-,'A-Za-z0-9 ]", "")
  })

  val replaceSpecial = udf((str: String) => {
    str.trim.replaceAll("[^\\p{L}\\p{Nd}]+", "")
  })

  val mkSrcRecordId = (df: DataFrame, col: Column) => {
    df.withColumn("src_record_id", col)
  }

  val mkSrcClaimId = (df: DataFrame, col: Column) => {
    df.withColumn("src_claim_id", col)
  }

  val mkSrcMemberId = (df: DataFrame, col: Column) => {
    df.withColumn("src_member_id", col)
  }

  //idiomatic scala way of mapping the fields
  def mapFields(df: DataFrame, fields:Map[String, String]): DataFrame = {
    //foldLeft is a curried function. this was confusing at first without the () around the anonymous CASE function
    fields.foldLeft(df)({ case (frame, (key, value)) => frame.withColumnRenamed(key, value) })
  }

}
