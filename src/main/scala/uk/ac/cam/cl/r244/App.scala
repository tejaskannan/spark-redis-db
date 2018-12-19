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

    db.countWithPrefix(table, "firstName", "ab")

    val t0 = System.nanoTime()
    println(db.countWithPrefix(table, "firstName", "ab"))
    val elapsed0 = (System.nanoTime() - t0) / 1000000.0
    println("Time to Exec Query: " + elapsed0.toString + "ms")

    Thread.sleep(1000)

    val t1 = System.nanoTime()
    println(db.getWithPrefix(table, "firstName", "ab").size)
    val elapsed1 = (System.nanoTime() - t1) / 1000000.0
    println("Time to Exec Query: " + elapsed1.toString + "ms")
  }

}
