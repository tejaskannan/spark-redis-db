package uk.ac.cam.cl.r244

/**
 * @author tejaskannan
 */

import com.redis.RedisClient
import scala.collection.immutable.{Map, List, Set}
import org.apache.spark.{sql, SparkConf, SparkContext}, sql.SparkSession
import com.redislabs.provider.redis._
import scala.util.{Success, Failure, matching}, matching.Regex
import scala.concurrent.{Future, Promise, Await, duration}, duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class RedisDatabase(_host: String, _port: Int) {
    val host: String = _host
    val port: Int = _port

    val redisClient = new RedisClient(host, port)
    val cacheManager = new CacheManager(16)

    private val keyFormat: String = "%s:%s"
    private val tableQueryFormat: String = "%s:*"
    private val cacheNameFormat: String = "%s:%s:%s"
    private val t: Int = 1000
    private val timeout: Duration = Duration(t, "millis")
    private val setRemoveThreshold: Int = 100

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

    def deleteTable(tableName: String): Long = {
        redisClient.del(tableName).get
    }

    def get(table: String, id: String): Map[String, String] = {
        val key: String = createKey(table, id)
        sanitizeData(redisClient.hgetall[String, String](key).get)
    }

    def countWithPrefix(table: String, field: String, prefix: String): Long = {
        val cacheFilter: String => Boolean = str => str(0) == prefix(0)
        countWith(table, field, str => str.startsWith(prefix), cacheFilter, "prefix:" + prefix(0).toString)
    }

    def countWithSuffix(table: String, field: String, suffix: String): Long = {
        val cacheFilter: String => Boolean = str => str(str.length - 1) == suffix(suffix.length - 1)
        countWith(table, field, str => str.endsWith(suffix), cacheFilter, "suffix:" + suffix(suffix.length - 1).toString)
    }

    def countWithRegex(table: String, field: String, regex: String): Long = {
        val r: Regex = regex.r
        countWith(table, field, str => r.findFirstIn(str) != None, str => true, "regex")
    }

    def getWithPrefix(table: String, field: String, prefix: String): List[Map[String, String]] = {
        val cacheFilter: String => Boolean = str => str(0) == prefix(0)
        getWith(table, field, str => str.startsWith(prefix), cacheFilter, "prefix:" + prefix(0).toString)
    }

    def getWithSuffix(table: String, field: String, suffix: String): List[Map[String, String]] = {
        val cacheFilter: String => Boolean = str => str(str.length - 1) == suffix(suffix.length - 1)
        getWith(table, field, str => str.endsWith(suffix), cacheFilter, "suffix:" + suffix(suffix.length - 1).toString)
    }

    def getWithRegex(table: String, field: String, regex: String): List[Map[String, String]] = {
        val r: Regex = regex.r
        getWith(table, field, str => r.findFirstIn(str) != None, str => true, "regex:")
    }

    private def countWith(table: String, field: String, filter: String => Boolean,
                          cacheFilter: String => Boolean, cacheName: String): Long = {
        val fullCacheName: String = createCacheName(table, field, cacheName)
        val fieldPrefix: String = keyFormat.format(field, "")

        // OPTIMIZATION: If the prefix/suffix is a single letter, don't even
        // use spark and just get the count of all redis indices in the cache
        if (!cacheManager.contains(fullCacheName)) {
            val hashRDD = spark.sparkContext.fromRedisHash(tableQueryFormat.format(table))

            val cacheRDD = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                                  .filter(entry => cacheFilter(entry._2))

            // Add indices to the cache. TODO: Make this writing asynchronous
            Future {
                spark.sparkContext.toRedisSET(cacheRDD.map(entry => entry._1.split(":")(1)),
                                              fullCacheName)
                cacheManager.add(fullCacheName, deleteTable)
            }

            cacheRDD.filter(entry => filter(entry._2)).count()
        } else {
            // We fetch the indices into memory as the set of indices is small
            val indices: Array[String] = redisClient.smembers[String](fullCacheName).get
                                                    .map(x => table + ":" + x.get)
                                                    .toArray
            val hashRDD = spark.sparkContext.fromRedisHash(indices)
            hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                   .filter(entry => filter(entry._2)).count()
        }
    }

    private def getWith(table: String, field: String, filter: String => Boolean,
                        cacheFilter: String => Boolean, cacheName: String): List[Map[String, String]] = {
        val fullCacheName: String = createCacheName(table, field, cacheName)
        val fieldPrefix: String = keyFormat.format(field, "")
        var ids: List[String] = List()
        val valueIndex: Int = 1

        if (!cacheManager.contains(fullCacheName)) {
            val hashRDD = spark.sparkContext.fromRedisHash(tableQueryFormat.format(table))

            val cacheRDD = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                                  .filter(entry => cacheFilter(entry._2))

            Future {
                spark.sparkContext.toRedisSET(cacheRDD.map(entry => entry._1.split(":")(valueIndex)),
                                              fullCacheName)
                cacheManager.add(fullCacheName, deleteTable)
            }

            ids = cacheRDD.filter(entry => filter(entry._2))
                          .map(entry => entry._1.split(":")(valueIndex))
                          .collect().toList
        } else {
            val indices: Array[String] = redisClient.smembers[String](fullCacheName).get
                                                    .map(x => table + ":" + x.get)
                                                    .toArray
            val hashRDD = spark.sparkContext.fromRedisHash(indices)
            ids = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                         .filter(entry => filter(entry._2))
                         .map(entry => entry._1.split(":")(valueIndex))
                         .collect().toList
        }

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

    private def createCacheName(table: String, field: String, name: String): String = {
        cacheNameFormat.format(table, field, name)
    }

}
