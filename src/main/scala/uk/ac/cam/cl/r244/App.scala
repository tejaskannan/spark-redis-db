package uk.ac.cam.cl.r244

/**
 * @author ${user.name}
 */

import scala.collection.immutable.{Map, List}

object App {

  def foo(x : Array[String]): String = x.foldLeft("")((a,b) => a + b)

  def main(args : Array[String]) {
    val db = new RedisDatabase("localhost", 6379)

    val table = "nfl"
    val id0 = "15"
    val data0 = Map(("firstName" -> "Patrick"), ("lastName" -> "Mahomes"))

    db.write(table, id0, data0)

    val id1 = "10"
    val data1 = Map(("firstName" -> "DeAndre"), ("lastName" -> "Hopkins"))
    db.write(table, id1, data1)

    println(db.getWithRegex(table, "lastName", "[M|H]*o*s"))

    println(db.countWithPrefix(table, "firstName", "D"))

    val t0 = System.nanoTime()
    println(db.getWithRegex(table, "lastName", "[M|H]*o*s"))
    val elapsed = (System.nanoTime() - t0) / 1000000.0
    println("Time to Create RDD: " + elapsed.toString + "ms")

    println(db.countWithSuffix(table, "lastName", "ns"))
    println(db.countWithRegex(table, "firstName", "P*tri+"))
  }

}
