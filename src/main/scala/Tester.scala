/**
  * Created by developer on 1/18/17.
  */
object Tester {

  def main(args: Array[String]): Unit = {

    object Test {
      def apply(): java.util.Date = new java.util.Date()
    }

    println(Test())
  }
}
