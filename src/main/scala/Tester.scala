/**
  * Created by developer on 1/18/17.
  */
import transform.ColumnTransformations
object Tester {

  def main(args: Array[String]): Unit = {

    object Test {
      def apply(): java.util.Date = new java.util.Date()
    }

    val t = ColumnTransformations

    val strs = Array("5944", "50000", "543", "403334")

    val out = strs.map(ele => t.icd9Format(ele))

  }
}
