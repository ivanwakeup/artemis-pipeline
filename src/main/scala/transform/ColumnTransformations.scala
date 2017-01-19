package transform

import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql._

/**
  * Created by developer on 1/17/17.
  */
object ColumnTransformations {


  val titleCase = udf((str: String) => str.toLowerCase.capitalize)
  val stripCommas = udf((str: String) => str.replaceAll(",", ""))

  //define date input/output formats
  val formatStr = udf((str: String) => {
    val inputFormat = new SimpleDateFormat("m-d-yyyy")
    val outputFormat = new SimpleDateFormat("mm-dd-yyyy")
    outputFormat.format(inputFormat.parse(str))
  })

  val pdToDash = udf((str: String) => {
    str.replaceAll("\\.", "-")
  })

  val pdStrip = udf((str: String) => {
    str.replaceAll("\\.", "")
  })

}
