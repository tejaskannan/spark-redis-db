package uk.ac.cam.cl.r244

/**
 * @author ${user.name}
 */

import scala.collection.immutable.{Map, List}

object App {

  def foo(x : Array[String]): String = x.foldLeft("")((a,b) => a + b)

  def main(args : Array[String]) {
    val db = new RedisDatabase("localhost", 6379)

    val table = "nflp"

    println(db.countWithPrefix(table, "firstName", "Patr"))

    val t0 = System.nanoTime()
    println(db.countWithPrefix(table, "firstName", "Ph"))
    val elapsed = (System.nanoTime() - t0) / 1000000.0
    println("Time to Exec Query: " + elapsed.toString + "ms")
  }

}
