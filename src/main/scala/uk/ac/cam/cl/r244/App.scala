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
    val id = "15"
    val data = Map(("firstName" -> "Patrick"), ("lastName" -> "Mahomes"))

    db.write(table, id, data)
    val dataFromDb: Option[Map[String, String]] = db.get(table, id)
    if (!dataFromDb.isEmpty) {
        println(dataFromDb)
    }

    val toDelete: List[String] = List("firstName")
    println(db.delete(table, id, List[String]()))
    val dataFromDbDeleted: Option[Map[String, String]] = db.get(table, id)
    println(dataFromDbDeleted)

  }

}
