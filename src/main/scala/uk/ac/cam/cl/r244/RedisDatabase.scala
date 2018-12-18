package uk.ac.cam.cl.r244

/**
 * @author tejaskannan
 */

import com.redis.RedisClient
import scala.collection.immutable.{Map, List}
import org.apache.spark.{sql, SparkConf, SparkContext}, sql.SparkSession
import com.redislabs.provider.redis._
import scala.util.matching.Regex
import scala.concurrent.{Future, Promise, Await, duration}, duration.Duration

class RedisDatabase(_host: String, _port: Int) {
    val host: String = _host
    val port: Int = _port

    val redisClient = new RedisClient(host, port)
    private val keyFormat: String = "%s:%s"
    private val tableQueryFormat: String = "%s:*"
    private val t: Int = 1000
    private val timeout: Duration = Duration(t, "millis")

    val sparkConf = new SparkConf().setMaster("local")
            .setAppName("spark-redis-db")
            .set("spark.redis.host", host)
            .set("spark.redis.port", port.toString)
            .set("spark.redis.auth", "")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    def write(table: String, id: String, data: Map[String, String]): Boolean = {
        val key: String = createKey(table, id)
        key != null && !key.isEmpty && redisClient.hmset(key, prepareData(data, id))
    }

    def delete(table: String, id: String, fields: List[String]): Option[Long] = {
        val key: String = createKey(table, id)
        if (!fields.isEmpty) {
            redisClient.hdel(key, fields.head, fields.tail)
        } else {
            redisClient.del(key)
        }
    }

    def get(table: String, id: String): Map[String, String] = {
        val key: String = createKey(table, id)
        sanitizeData(redisClient.hgetall[String, String](key).get)
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

    def getWithPrefix(table: String, field: String, prefix: String): List[Map[String, String]] = {
        getWith(table, field, str => str.startsWith(prefix))
    }

    def getWithSuffix(table: String, field: String, suffix: String): List[Map[String, String]] = {
        getWith(table, field, str => str.endsWith(suffix))
    }

    def getWithRegex(table: String, field: String, regex: String): List[Map[String, String]] = {
        val r: Regex = regex.r
        getWith(table, field, str => r.findFirstIn(str) != None)
    }

    private def countWith(table: String, field: String, filter: String => Boolean): Long = {
        val hashRDD = spark.sparkContext.fromRedisHash(tableQueryFormat.format(table))
        val fieldPrefix: String = keyFormat.format(field, "")
        hashRDD.filter(entry => entry._1.startsWith(fieldPrefix)).filter(entry => filter(entry._2)).count()
    }

    private def getWith(table: String, field: String, filter: String => Boolean): List[Map[String, String]] = {
        val hashRDD = spark.sparkContext.fromRedisHash(tableQueryFormat.format(table))

        val fieldPrefix: String = keyFormat.format(field, "")
        val valueIndex: Int = 1

        val ids: List[String] = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                                       .filter(entry => filter(entry._2))
                                       .map(entry => entry._1.split(":")(valueIndex))
                                       .collect().toList

        val queries = ids.map(id => keyFormat.format(table, id))
                         .map(key => (() => redisClient.hgetall[String, String](key)))

        val queryExec = redisClient.pipelineNoMulti(queries)
        queryExec.map(a => Await.result(a.future, timeout))
                 .map(_.asInstanceOf[Option[Map[String, String]]])
                 .map(m => sanitizeData(m.get))
    }

    private def prepareData(data: Map[String, String], id: String): Map[String, String] = {
        data.map(entry => (keyFormat.format(entry._1, id), entry._2))
    }

    private def sanitizeData(data: Map[String, String]): Map[String, String] = {
        data.map(entry => (entry._1.split(":")(0), entry._2))
    }

    private def createKey(table: String, id: String): String = {
        keyFormat.format(table, id)
    }
}
