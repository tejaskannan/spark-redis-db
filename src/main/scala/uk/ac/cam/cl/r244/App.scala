package uk.ac.cam.cl.r244

/**
 * @author ${user.name}
 */

import scala.collection.immutable.{Map, List}

object App {

  def main(args : Array[String]) {
    val db = new RedisDatabase("localhost", 6379)

    val table = "nflp"

    db.countWithPrefix(table, "firstName", "ab")

    val t0 = System.nanoTime()
    println(db.countWithPrefix(table, "firstName", "patr"))
    val elapsed0 = (System.nanoTime() - t0) / 1000000.0
    println("Time to Exec Query: " + elapsed0.toString + "ms")

    Thread.sleep(3000)

    val t1 = System.nanoTime()
    println(db.getWithRegex(table, "firstName", "^.*e.*a.*$").size)
    val elapsed1 = (System.nanoTime() - t1) / 1000000.0
    println("Time to Exec Query: " + elapsed1.toString + "ms")

    val t2 = System.nanoTime()
    println(db.getWithRegex(table, "firstName", "^.*e.*z.*$").size)
    val elapsed2 = (System.nanoTime() - t2) / 1000000.0
    println("Time to Exec Query: " + elapsed2.toString + "ms")
  }

}
