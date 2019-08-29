import java.text.SimpleDateFormat

object Exer {
  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    val timestamp: Long = format.parse("17/05/2015:10:05:03").getTime
    println(timestamp)
  }

}
