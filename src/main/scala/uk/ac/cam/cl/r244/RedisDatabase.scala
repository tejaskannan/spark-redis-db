package uk.ac.cam.cl.r244

/**
 * @author tejaskannan
 */

import com.redis.RedisClient
import scala.collection.immutable.{Map, List, Set}
import scala.collection.mutable.ListBuffer
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
    val statsManager = new StatisticsManager()

    private val keyFormat: String = "%s:%s"
    private val tableQueryFormat: String = "%s:*"
    private val cacheIdFormat: String = "%s:%s"
    private val cacheNameFormat: String = "%s:%s:%s"
    private val t: Int = 1000
    private val timeout: Duration = Duration(t, "millis")
    private val setRemoveThreshold: Int = 100

    private val prefixName: String = "prefix"
    private val suffixName: String = "suffix"
    private val regexName: String = "contains"
    private val queryTypes: List[String] = List(prefixName, suffixName, regexName)

    val sparkConf = new SparkConf().setMaster("local")
            .setAppName("spark-redis-db")
            .set("spark.redis.host", host)
            .set("spark.redis.port", port.toString)
            .set("spark.redis.auth", "")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    var sparkContext = spark.sparkContext

    def write(table: String, id: String, data: Map[String, String]): Boolean = {
        // TODO: Add this index to relevant caches
        val key: String = createKey(table, id)
        key != null && !key.isEmpty && redisClient.hmset(key, prepareData(data, id))
    }

    def delete(table: String, id: String): Option[Long] = {
        val key: String = createKey(table, id)
        removeIdFromCaches(table, id)
        redisClient.del(key)
    }

    def deleteCache(cacheName: String): Unit = {
        redisClient.del(cacheName)
    }

    def get(table: String, id: String): Map[String, String] = {
        val key: String = createKey(table, id)
        sanitizeData(redisClient.hgetall[String, String](key).get)
    }

    def countWithPrefix(table: String, field: String, prefix: String): Long = {
        val cacheFilter: String => Boolean = str => str(0) == prefix(0)
        val cacheId: String = cacheIdFormat.format(prefixName, prefix(0).toString)
        countWith(table, field, str => str.startsWith(prefix), cacheFilter, cacheId, prefix.length == 1)
    }

    def countWithSuffix(table: String, field: String, suffix: String): Long = {
        val cacheFilter: String => Boolean = str => str(str.length - 1) == suffix(suffix.length - 1)
        val cacheId: String = cacheIdFormat.format(suffixName, suffix(suffix.length - 1).toString)
        countWith(table, field, str => str.endsWith(suffix), cacheFilter, cacheId, suffix.length == 1)
    }

    def countWithRegex(table: String, field: String, regex: String): Long = {
        val r: Regex = regex.r
        val cacheId: String = findRegexCacheName(regex)
        val maxChar: Char = Utils.getMaxFreqLetter(regex, statsManager)
        val singleLetter: Boolean = regex.filter(c => Utils.isLetter(c)).size == 1
        countWith(table, field, str => r.findFirstIn(str) != None, str => str.contains(maxChar), cacheId, singleLetter)
    }

    def getWithPrefix(table: String, field: String, prefix: String): List[Map[String, String]] = {
        val cacheFilter: String => Boolean = str => str(0) == prefix(0)
        val cacheId: String = cacheIdFormat.format(prefixName, prefix(0).toString)
        getWith(table, field, str => str.startsWith(prefix), cacheFilter, cacheId)
    }

    def getWithSuffix(table: String, field: String, suffix: String): List[Map[String, String]] = {
        val cacheFilter: String => Boolean = str => str(str.length - 1) == suffix(suffix.length - 1)
        val cacheId: String = cacheIdFormat.format(suffixName, suffix(suffix.length - 1).toString)
        getWith(table, field, str => str.endsWith(suffix), cacheFilter, cacheId)
    }

    def getWithRegex(table: String, field: String, regex: String): List[Map[String, String]] = {
        val r: Regex = regex.r
        val cacheId: String = findRegexCacheName(regex)
        val maxChar: Char = Utils.getMaxFreqLetter(regex, statsManager)
        getWith(table, field, str => r.findFirstIn(str) != None, str => str.contains(maxChar), cacheId)
    }

    private def countWith(table: String, field: String, filter: String => Boolean,
                          cacheFilter: String => Boolean, cacheName: String,
                          singleLetter: Boolean): Long = {
        val fullCacheName: String = createCacheName(table, field, cacheName)
        val fieldPrefix: String = keyFormat.format(field, "")

        if (!cacheManager.contains(fullCacheName)) {
            val hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))

            val cacheRDD = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix) && cacheFilter(entry._2))
            
            // Add indices to the cache asychronously
            Future {
                sparkContext.toRedisSET(cacheRDD.map(entry => entry._1.split(":")(1)),
                                              fullCacheName)
                cacheManager.add(fullCacheName, deleteCache)
            }

            cacheRDD.filter(entry => filter(entry._2)).count()
        } else {
            // If the query is only a single letter, we can get the count directly from
            // the cache
            if (singleLetter) {
                redisClient.scard(fullCacheName).get
            } else {
                // We fetch the indices into memory as the set of indices is small
                val indices: Array[String] = redisClient.smembers[String](fullCacheName).get
                                                        .map(x => table + ":" + x.get)
                                                        .toArray

                val hashRDD = sparkContext.fromRedisHash(indices)
                hashRDD.filter(entry => entry._1.startsWith(fieldPrefix) && filter(entry._2))
                       .count()
            }
        }
    }

    private def getWith(table: String, field: String, filter: String => Boolean,
                        cacheFilter: String => Boolean, cacheName: String): List[Map[String, String]] = {
        val fullCacheName: String = createCacheName(table, field, cacheName)
        val fieldPrefix: String = keyFormat.format(field, "")
        var ids: List[String] = List()
        val valueIndex: Int = 1

        if (!cacheManager.contains(fullCacheName)) {
            val hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))

            val cacheRDD = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix) && cacheFilter(entry._2))

            // Add indices to the cache asychronously
            Future {
                sparkContext.toRedisSET(cacheRDD.map(entry => entry._1.split(":")(valueIndex)),
                                              fullCacheName)
                cacheManager.add(fullCacheName, deleteCache)
            }

            ids = cacheRDD.filter(entry => filter(entry._2))
                          .map(entry => entry._1.split(":")(valueIndex))
                          .collect().toList
        } else {
            val indices: Array[String] = redisClient.smembers[String](fullCacheName).get
                                                    .map(x => table + ":" + x.get)
                                                    .toArray
            val hashRDD = sparkContext.fromRedisHash(indices)
            ids = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix) && filter(entry._2))
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

    private def removeIdFromCaches(table: String, id: String): Unit = {
        for (queryType <- queryTypes) {
            val cacheNames: List[String] = cacheManager.getCacheNamesWithPrefix(table + queryType)
            val queries = cacheNames.map(name => (() => redisClient.srem(name, id)))
            redisClient.pipelineNoMulti(queries)
        }
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

    private def findRegexCacheName(regex: String): String = {
        val len: Int = regex.length
        if (Utils.isLetter(regex(0))) {
            cacheIdFormat.format(prefixName, regex(0).toString)
        } else if (regex(0) == '^' && Utils.isLetter(regex(1))) {
            cacheIdFormat.format(prefixName, regex(1).toString)
        } else if (Utils.isLetter(regex(len - 1))) {
            cacheIdFormat.format(suffixName, regex(len - 1).toString)
        } else if (regex(len - 1) == '$' && Utils.isLetter(regex(len - 2))) {
            cacheIdFormat.format(suffixName, regex(len - 2).toString)
        } else {
            val maxChar: Char = Utils.getMaxFreqLetter(regex, statsManager)
            cacheIdFormat.format(regexName, maxChar.toString)
        }
    }

}
