package uk.ac.cam.cl.r244

/**
 * @author ${user.name}
 */

import scala.collection.immutable.{Map, List}
import java.util.Random

object App {

  def main(args : Array[String]) {
    val db = new RedisDatabase("localhost", 6379)

    val table = "dna"
    val field = args(0)

    val strings: Array[String] = Array("gat[t|a]aca", "gat.+t")
    val rand = new Random(System.currentTimeMillis())
    val trials = args(1).toInt
    db.countWithPrefix(table, field, "ov", false)

    println("Starting Query")
    val t0 = System.currentTimeMillis()
    for (i <- 0 until trials) {
        val str = strings(rand.nextInt(strings.length))
        println(db.countWithRegex(table, field, str, false))
    }
    val elapsed0 = (System.currentTimeMillis() - t0)
    println("Time to Exec Query: " + elapsed0.toString + "ms") 
  }
}
