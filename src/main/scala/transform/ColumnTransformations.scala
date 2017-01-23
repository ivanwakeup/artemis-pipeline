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

  val replaceSpecial = udf((str: String) => {
    str.trim.replaceAll("[^\\p{L}\\p{Nd}]+", "")
  })

  
}
