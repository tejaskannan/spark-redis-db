package uk.ac.cam.cl.r244

/**
 * @author tejaskannan
 */

import com.redis.RedisClient
import scala.collection.immutable.{Map, List}

class RedisDatabase(_host: String, _port: Int) {
    val host: String = _host
    val port: Int = _port

    val redisClient = new RedisClient(host, port)
    private val keyFormat: String = "%s:%s"

    def write(table: String, id: String, data: Map[String, String]): Boolean = {
        val key: String = createKey(table, id)
        if (key == null || key.length == 0) false else redisClient.hmset(key, data)
    }

    def get(table: String, id: String): Option[Map[String, String]] = {
        val key: String = createKey(table, id)
        redisClient.hgetall[String, String](key)
    }

    def delete(table: String, id: String, fields: List[String]): Option[Long] = {
        val key: String = createKey(table, id)
        if (!fields.isEmpty) {
            println("Fields Given")
            redisClient.hdel(key, fields.head, fields.tail)
        } else {
            redisClient.del(key)
        }
    }

    private def createKey(table: String, id: String): String = {
        keyFormat.format(table, id)
    }

}
