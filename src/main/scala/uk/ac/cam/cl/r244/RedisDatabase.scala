package uk.ac.cam.cl.r244

/**
 * @author tejaskannan
 */

import com.redis.RedisClient
import scala.collection.immutable.{Map, List}
import org.apache.spark.{sql, SparkConf, SparkContext}, sql.SparkSession
import com.redislabs.provider.redis._
import scala.util.matching.Regex

class RedisDatabase(_host: String, _port: Int) {
    val host: String = _host
    val port: Int = _port

    val redisClient = new RedisClient(host, port)
    private val keyFormat: String = "%s:%s"
    private val tableQueryFormat: String = "%s:*"

    val sparkConf = new SparkConf().setMaster("local")
            .setAppName("spark-redis-db")
            .set("spark.redis.host", host)
            .set("spark.redis.port", port.toString)
            .set("spark.redis.auth", "")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    def write(table: String, id: String, data: Map[String, String]): Boolean = {
        val key: String = createKey(table, id)
        if (key == null || key.length == 0) false else redisClient.hmset(key, data)
    }

    def get(table: String, id: String): Option[Map[String, String]] = {
        val key: String = createKey(table, id)
        redisClient.hgetall[String, String](key)
    }

    def countWithPrefix(table: String, field: String, prefix: String): Long = {
        countWith(table, field, str => str.startsWith(prefix))
    }

    def countWithSuffix(table: String, field: String, suffix: String): Long = {
        countWith(table, field, str => str.endsWith(suffix))
    }

    def countWithRegex(table: String, field: String, regex: String): Long = {
        val r: Regex = regex.r
        countWith(table, field, str => r.findFirstIn(str) != None) 
    }

    def countWith(table: String, field: String, filter: String => Boolean): Long = {
        val hashRDD = spark.sparkContext.fromRedisHash(tableQueryFormat.format(table))
        hashRDD.filter(entry => entry._1 == field).filter(entry => filter(entry._2)).count()
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
