package transform

import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql._

/**
  * Created by developer on 1/17/17.
  */
class ColumnTransformations(session: SparkSession) {

  {
    val funcs = Map("titleCase" -> titleCase
      , "stripCommas" -> stripCommas
      ,"formatStr" -> formatStr
      ,"pdToDash" -> pdToDash
      ,"pdStrip" -> pdStrip
      ,"timesTwo" -> timesTwo)
    val sesh = session
    registerFuncs(funcs, sesh)
  }
  val titleCase: String => String = _.toLowerCase.capitalize
  val stripCommas: String => String = _.replaceAll(",", "")

  //define date input/output formats
  val formatStr = (str: String) => {
    val inputFormat = new SimpleDateFormat("m-d-yyyy")
    val outputFormat = new SimpleDateFormat("mm-dd-yyyy")
    outputFormat.format(inputFormat.parse(str))
  }

  val pdToDash = (str: String) => {
    str.replaceAll("\\.", "-")
  }

  val pdStrip = (str: String) => {
    str.replaceAll("\\.", "")
  }
  val timesTwo = (x: Int) => {
    x * 2
  }

  val fields = ColumnTransformations.getClass.getDeclaredFields

  val fieldNames = ColumnTransformations.getClass.getDeclaredFields.map(ele => ele.toString)
  val fieldName = fields(0).get()

  def registerFuncs(funcs: Map[String ->], sesh: SparkSession): Unit = {
    for(func <- funcs) {
      //sesh.udf.register(func.
    }
  }
}
object ColumnTransformations {

}
