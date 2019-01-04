package uk.ac.cam.cl.r244

/**
 * @author ${user.name}
 */

import scala.collection.immutable.{Map, List}
import java.util.Random

object App {

  def main(args : Array[String]) {
    val db = new RedisDatabase("localhost", 6379)

    val table = "movies"
    val field = args(0)

    val prefixes: Array[String] = Array("an", "anti", "auto", "de", "dis", "mis", "non", "out", "post",
                                        "p", "pro", "semi", "sub", "ultra", "un")
    val rand = new Random(System.currentTimeMillis())
    val trials = args(1).toInt
    db.countWithPrefix(table, field, "ov")

    println("Starting Query")
    val t0 = System.currentTimeMillis()
    for (i <- 0 until trials) {
        val prefix = prefixes(rand.nextInt(prefixes.length))
        println(prefix)
        db.getWithPrefix(table, field, prefix)
    }
    val elapsed0 = (System.currentTimeMillis() - t0)
    println("Time to Exec Query: " + elapsed0.toString + "ms") 



    // val table = "nflp"

    // db.countWithPrefix(table, "firstName", "ab")

    // val t0 = System.nanoTime()
    // println(db.countWithContains(table, "firstName", "ame"))
    // val elapsed0 = (System.nanoTime() - t0) / 1000000.0
    // println("Time to Exec Query: " + elapsed0.toString + "ms")

    // Thread.sleep(1000)

    // val t1 = System.nanoTime()
    // println(db.countWithContains(table, "firstName", "men"))
    // val elapsed1 = (System.nanoTime() - t1) / 1000000.0
    // println("Time to Exec Query: " + elapsed1.toString + "ms")
  }
}
