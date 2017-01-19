package formats

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by developer on 1/19/17.
  */
class FixedWidthFile(filepath: String, session: SparkSession, widths: Array[Int], fields: Array[String] = null) extends java.io.Serializable {

  //kind of dumb but you NEED the schema defined to do the below mapping
  //seems ridiculous to define a schema and then map it to Dataset[Row] though. should find a better way to do this.
  val schemaArr = if (fields == null) widths.map(ele => ele.toString) else fields
  val schema = StructType(schemaArr.map(field => StructField(field, StringType, nullable = true)))
  val encoder = RowEncoder(schema)


  def toDataFrame: DataFrame = {
    session.read.textFile(filepath)
      .map(line  => fixedLineToRow(line, widths))(encoder)
      .toDF()
  }

  def fixedLineToRow(x: String, widths: Array[Int]): Row = {
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

}
