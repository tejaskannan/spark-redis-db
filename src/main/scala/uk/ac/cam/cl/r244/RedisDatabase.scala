package uk.ac.cam.cl.r244

/**
 * @author tejaskannan
 */

import com.redis.RedisClient
import scala.collection.immutable.{Map, List, Set}
import scala.collection.mutable.ListBuffer
import org.apache.spark.{sql, SparkConf, SparkContext, rdd}, sql.SparkSession, rdd.RDD
import com.redislabs.provider.redis._
import scala.util.{Success, Failure, matching}, matching.Regex
import scala.concurrent.{Future, Promise, Await, duration}, duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class RedisDatabase(_host: String, _port: Int) {
    val host: String = _host
    val port: Int = _port

    val redisClient = new RedisClient(host, port)
    val statsManager = new StatisticsManager()
    val cacheSize = 16
    val cacheManager = new CacheManager(cacheSize, statsManager)

    private val keyFormat: String = "%s:%s"
    private val tableQueryFormat: String = "%s:*"
    private val cacheIdFormat: String = "%s:%s"
    private val cacheNameFormat: String = "%s:%s:%s"
    private val wordsRegex: Regex = "\\s+".r
    private val t: Int = 1000
    private val timeout: Duration = Duration(t, "millis")
    private val setRemoveThreshold: Int = 100
    private val maxFreqCutoff: Int = 256

    private val prefixName: String = "prefix"
    private val suffixName: String = "suffix"
    private val regexName: String = "contains"
    private val editDistName: String = "editdist"
    private val queryTypes: List[String] = List(prefixName, suffixName, regexName)

    val sparkConf = new SparkConf().setMaster("local")
            .setAppName("spark-redis-db")
            .set("spark.redis.host", host)
            .set("spark.redis.port", port.toString)
            .set("spark.redis.auth", "")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    var sparkContext = spark.sparkContext

    def write(table: String, id: String, data: Map[String, String]): Boolean = {
        val key: String = createKey(table, id)

        val existingRecord: Map[String, String] = get(table, id)
        updateCaches(table, id, existingRecord, data)

        statsManager.addWrite()
        statsManager.addCountToTable(table)

        key != null && !key.isEmpty && redisClient.hmset(key, prepareData(data, id))
    }

    def delete(table: String, id: String): Option[Long] = {
        val key: String = createKey(table, id)
        removeIdFromCaches(table, id)
        statsManager.addDelete()
        statsManager.removeCountFromTable(table)
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
        countWith(table, field, str => str.startsWith(prefix), cacheFilter,
                  cacheId, prefix.length == 1, wordsRegex.findFirstIn(prefix) != None)
    }

    def countWithSuffix(table: String, field: String, suffix: String): Long = {
        val cacheFilter: String => Boolean = str => str(str.length - 1) == suffix(suffix.length - 1)
        val cacheId: String = cacheIdFormat.format(suffixName, suffix(suffix.length - 1).toString)
        countWith(table, field, str => str.endsWith(suffix), cacheFilter,
                  cacheId, suffix.length == 1, wordsRegex.findFirstIn(suffix) != None)
    }

    def countWithRegex(table: String, field: String, regex: String): Long = {
        val r: Regex = regex.r
        val cacheId: String = findRegexCacheName(regex)
        val maxChar: Char = Utils.getMaxFreqLetter(regex, statsManager, maxFreqCutoff)
        val singleLetter: Boolean = regex.filter(c => Utils.isLetter(c)).size == 1
        countWith(table, field, str => r.findFirstIn(str) != None, str => str.contains(maxChar),
                  cacheId, singleLetter, true)
    }

    def countWithEditDistance(table: String, field: String, target: String, dist: Int): Long = {
        val minLength: Int = target.length - dist
        val maxLength: Int = target.length + dist
        val cacheId: String = cacheIdFormat.format(editDistName, keyFormat.format(minLength.toString, maxLength.toString))
        val cacheFilter: String => Boolean = str => (target.length - dist <= str.length && str.length <= target.length + dist)
        countWith(table, field, str => Utils.editDistance(str, target, dist), cacheFilter,
                  cacheId, false, wordsRegex.findFirstIn(target) != None)
    }

    def countWithContains(table: String, field: String, substring: String): Long = {
        val maxChar: Char = Utils.getMaxFreqLetter(substring, statsManager, maxFreqCutoff)
        val cacheFilter: String => Boolean = str => str.contains(maxChar)
        val filter: String => Boolean = str => str.contains(substring)
        val cacheId: String = cacheIdFormat.format(regexName, maxChar)
        val multiWord: Boolean = wordsRegex.findFirstIn(substring) != None
        countWith(table, field, filter, cacheFilter, cacheId, substring.length == 1, multiWord)
    }

    def getWithPrefix(table: String, field: String, prefix: String): List[Map[String, String]] = {
        val cacheFilter: String => Boolean = str => str(0) == prefix(0)
        val cacheId: String = cacheIdFormat.format(prefixName, prefix(0).toString)
        val multiWord: Boolean = wordsRegex.findFirstIn(prefix) != None
        getWith(table, field, str => str.startsWith(prefix), cacheFilter, cacheId, multiWord)
    }

    def getWithSuffix(table: String, field: String, suffix: String): List[Map[String, String]] = {
        val cacheFilter: String => Boolean = str => str(str.length - 1) == suffix(suffix.length - 1)
        val cacheId: String = cacheIdFormat.format(suffixName, suffix(suffix.length - 1).toString)
        val multiWord: Boolean = wordsRegex.findFirstIn(suffix) != None
        getWith(table, field, str => str.endsWith(suffix), cacheFilter, cacheId, multiWord)
    }

    def getWithRegex(table: String, field: String, regex: String): List[Map[String, String]] = {
        val r: Regex = regex.r
        val cacheId: String = findRegexCacheName(regex)
        val maxChar: Char = Utils.getMaxFreqLetter(regex, statsManager, maxFreqCutoff)
        getWith(table, field, str => r.findFirstIn(str) != None, str => str.contains(maxChar),
                cacheId, true)
    }

    def getWithEditDistance(table: String, field: String, target: String, dist: Int): List[Map[String, String]] = {
        val minLength: Int = target.length - dist
        val maxLength: Int = target.length + dist
        val cacheId: String = cacheIdFormat.format(editDistName, keyFormat.format(minLength.toString, maxLength.toString))
        val cacheFilter: String => Boolean = str => (minLength <= str.length && str.length <= maxLength)
        val multiWord: Boolean = wordsRegex.findFirstIn(target) != None
        getWith(table, field, str => Utils.editDistance(str, target, dist), cacheFilter, cacheId, multiWord)
    }

    def getWithContains(table: String, field: String, substring: String): List[Map[String, String]] = {
        val maxChar: Char = Utils.getMaxFreqLetter(substring, statsManager, maxFreqCutoff)
        val cacheFilter: String => Boolean = str => str.contains(maxChar)
        val filter: String => Boolean = str => str.contains(substring)
        val cacheId: String = cacheIdFormat.format(regexName, maxChar)
        val multiWord: Boolean = wordsRegex.findFirstIn(substring) != None
        getWith(table, field, filter, cacheFilter, cacheId, multiWord)
    }

    private def countWith(table: String, field: String, filter: String => Boolean,
                          cacheFilter: String => Boolean, cacheName: String,
                          singleLetter: Boolean, multiWord: Boolean): Long = {
        val fullCacheName: String = createCacheName(table, field, cacheName)
        val fieldPrefix: String = keyFormat.format(field, "")

        statsManager.addRead()

        if (!cacheManager.contains(fullCacheName)) {
            val hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))

            var cacheRDD: RDD[(String, String)] = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
            if (!multiWord) {
                cacheRDD = cacheRDD.flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toList)
            }
            cacheRDD = cacheRDD.filter(entry => cacheFilter(entry._2))
            
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
                val count: Long = redisClient.scard(fullCacheName).get
                statsManager.addCacheHit(fullCacheName, table, count.toInt)
                count
            } else {
                // We fetch the indices into memory as the set of indices is small
                val indices: Array[String] = redisClient.smembers[String](fullCacheName).get
                                                        .map(x => keyFormat.format(table, x.get))
                                                        .toArray

                statsManager.addCacheHit(fullCacheName, table, indices.size)

                val hashRDD = sparkContext.fromRedisHash(indices)
                hashRDD.filter(entry => entry._1.startsWith(fieldPrefix) && filter(entry._2))
                       .count()
            }
        }
    }

    private def getWith(table: String, field: String, filter: String => Boolean,
                        cacheFilter: String => Boolean, cacheName: String,
                        multiWord: Boolean): List[Map[String, String]] = {
        val fullCacheName: String = createCacheName(table, field, cacheName)
        val fieldPrefix: String = keyFormat.format(field, "")
        var ids: List[String] = List()
        val valueIndex: Int = 1

        statsManager.addRead()

        if (!cacheManager.contains(fullCacheName)) {
            val hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))

            var cacheRDD: RDD[(String, String)] = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
            if (!multiWord) {
                cacheRDD = cacheRDD.flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toList)
            }
            cacheRDD = cacheRDD.filter(entry => cacheFilter(entry._2))

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
                                                    .map(x => keyFormat.format(table, x.get))
                                                    .toArray

            statsManager.addCacheHit(fullCacheName, table, indices.size)

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
        val cacheNames: List[String] = cacheManager.getCacheNamesWithPrefix(table)
        cacheNames.foreach(name => redisClient.srem(name, id))
    }

    private def updateCaches(table: String, id: String, oldData: Map[String, String],
                             newData: Map[String, String]): List[Long] = {
        val fieldsToUpdate = newData.keys.toList
        for (field <- fieldsToUpdate) {
            val cacheNames: List[String] = cacheManager.getCacheNamesWithPrefix(table + ":" + field)
            cacheNames.foreach(name => redisClient.srem(name, id))
        }

        val prefixes = newData.map(e => (e._1, e._2(0)))
                              .map(e => (cacheNameFormat.format(table, e._1, prefixName + ":" + e._2), e._2))
                              .filter(e => cacheManager.contains(e._1))
                              .keys.toList
        val prefixAdds = prefixes.map(name => (() => redisClient.sadd(name, id)))

        val suffixes = newData.map(e => (e._1, e._2(e._2.length - 1)))
                              .map(e => (cacheNameFormat.format(table, e._1, suffixName + ":" + e._2), e._2))
                              .filter(e => cacheManager.contains(e._1))
                              .keys.toList
        val suffixAdds = suffixes.map(name => (() => redisClient.sadd(name, id)))

        val containsCaches = new ListBuffer[String]()
        for (field <- fieldsToUpdate) {
            val name: String = cacheNameFormat.format(table, field, regexName)
            val cacheNames: List[String] = cacheManager.getCacheNamesWithPrefix(name)
            val fieldData: String = newData(field).substring(1, newData(field).length - 1)
            for (cacheName <- cacheNames) {
                if (fieldData.contains(cacheName(cacheName.length - 1))) {
                    containsCaches += cacheName
                }
            }
        }

        val containsAdds = containsCaches.map(name => (() => redisClient.sadd(name, id)))
        val addExec = redisClient.pipelineNoMulti(prefixAdds ++ suffixAdds ++ containsAdds)
        addExec.map(a => Await.result(a.future, timeout * 5))
               .map(_.asInstanceOf[Option[Long]])
               .map(_.get)
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
            val maxChar: Char = Utils.getMaxFreqLetter(regex, statsManager, maxFreqCutoff)
            cacheIdFormat.format(regexName, maxChar.toString)
        }
    }

}
