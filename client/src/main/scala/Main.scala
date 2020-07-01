import java.util.Locale

import com.lsm.thrift.Service
import com.lsm.thrift.Service.{Delete, Get, Put}
import com.twitter.finagle.Thrift
import com.twitter.util.Future

import scala.util.Random

object RandomString {

  val upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val lower: String = upper.toLowerCase(Locale.ROOT)
  val digits = "0123456789"
  val alphanum: String = upper + lower + digits

  /**
   * Generate a random string.
   */
  def nextString(random: scala.util.Random, length: Int): String = {
    val buf = new StringBuilder()
    for (_ <- 0 until length) {
      buf += alphanum(random.nextInt(alphanum.length))
    }
    buf.toString
  }

}

object Main {

  private def getNextKey(random: Random): String = {
    RandomString.nextString(random, 10)
  }

  def generateCommand(cli: Service.ServicePerEndpoint, i: Int, random: scala.util.Random): Any = {
    val key = getNextKey(random)

    val j = i + 1
    random.nextInt(3) match {
      case 0 =>
        val opt: Future[Get.SuccessType] = cli.get(Get.Args(key))
        opt.onSuccess(v => println(String.format("%s GET: %s", j, v.toString)))
        opt.onFailure(_ => println(String.format("%d GET: No Such Element", j)))

      case 1 =>
        put(cli, random, key, j)

      case 2 =>
        val opt: Future[Delete.SuccessType] = cli.delete(Delete.Args(key))
        opt.onSuccess( _ => println(String.format("%d DELETE: %s", j, key)))
        opt.onFailure(_ => println(String.format("%d DELETE: No Such Element", j)))

    }
  }

  def initDB(cli: Service.ServicePerEndpoint, random: scala.util.Random, range: Range): Unit = {
    range.foreach(f => {
      var key = getNextKey(random)
      while (key contains ",") key = getNextKey(random)
      put(cli, random, key, f)
    })
  }

  def put(cli: Service.ServicePerEndpoint, random: Random, key: String, j: Int): Future[Unit] = {
    val value = random.nextInt(10e6.toInt)
    val opt: Future[Put.SuccessType] = cli.put(Put.Args(key, value))
    opt.onSuccess(_ => println(String.format("%d PUT: %s,%d", j, key, value)))
  }

  def main(args: Array[String]): Unit = {
    val cli: Service.ServicePerEndpoint =
      Thrift.client.servicePerEndpoint[Service.ServicePerEndpoint](
        "localhost:1234",
        "thrift_client"
      )

    val transactions = 1000000

    val r = new scala.util.Random

    (0 until transactions).foreach(f => {
      generateCommand(cli, f, r)
    })
  }
}
