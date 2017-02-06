/**
  * Created by developer on 1/18/17.
  */
import formats.FixedWidthFile
import org.apache.spark.sql.{DataFrame, SparkSession}
import transform.ColumnTransformations

import scala.collection.mutable
object Tester {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName("Test")
      .getOrCreate()

   /* object Test {
      def apply(): java.util.Date = new java.util.Date()
    }*/

  }
}
