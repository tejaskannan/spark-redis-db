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
    val cacheSize = 64
    val cacheManager = new CacheManager(cacheSize, statsManager)
    val stopwords = new Stopwords()

    private val keyFormat: String = "%s:%s"
    private val tableQueryFormat: String = "%s:*"
    private val cacheIdFormat: String = "%s:%s"
    private val cacheNameFormat: String = "%s:%s:%s"
    private val wordsRegex: Regex = "\\s+".r
    private val t: Int = 5000
    private val timeout: Duration = Duration(t, "millis")
    private val setRemoveThreshold: Int = 100
    private val maxFreqCutoff: Int = 256

    private val prefixName: String = "prefix"
    private val suffixName: String = "suffix"
    private val containsName: String = "contains"
    private val editDistName: String = "editdist"
    private val queryTypes: List[String] = List(prefixName, suffixName, containsName)

    val sparkConf = new SparkConf().setMaster("local[4]")
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

    def countWithPrefix(table: String, field: String, prefix: String,
                        multiWord: Boolean = false): Long = {
        val cacheFilter: String => Boolean = str => str(0) == prefix(0)
        var cacheChars: String = prefix(0).toString
        if (prefix.length > 1) {
            cacheChars += prefix(1).toString
        }
        val cacheId: String = cacheIdFormat.format(prefixName, cacheChars)
        countWith(table, field, str => str.startsWith(prefix), cacheFilter,
                  cacheId, multiWord)
    }

    def countWithSuffix(table: String, field: String, suffix: String,
                        multiWord: Boolean = false): Long = {
        val cacheFilter: String => Boolean = str => str(str.length - 1) == suffix(suffix.length - 1)
        var cacheChars: String = suffix(suffix.length - 1).toString
        if (suffix.length > 1) {
            cacheChars = suffix(suffix.length - 2).toString + cacheChars
        }
        val cacheId: String = cacheIdFormat.format(suffixName, cacheChars)
        countWith(table, field, str => str.endsWith(suffix), cacheFilter,
                  cacheId, multiWord)
    }

    def countWithRegex(table: String, field: String, regex: String,
                       multiWord: Boolean = false): Long = {
        val r: Regex = regex.r
        val cacheId: String = findRegexCacheName(regex)
        val cacheSubstr: String = Utils.getLongestCharSubstring(regex)
        countWith(table, field, str => r.findFirstIn(str) != None, str => str.contains(cacheSubstr),
                  cacheId, multiWord)
    }

    def countWithEditDistance(table: String, field: String, target: String,
                              dist: Int, multiWord: Boolean = false): Long = {
        val minLength: Int = Utils.max(target.length - dist, 0)
        val maxLength: Int = target.length + dist
        val cacheId: String = cacheIdFormat.format(editDistName, keyFormat.format(minLength.toString, maxLength.toString))
        val cacheFilter: String => Boolean = str => (minLength <= str.length && str.length <= maxLength)
        countWith(table, field, str => Utils.editDistance(str, target, dist), cacheFilter,
                  cacheId, multiWord)
    }

    def getWithPrefix(table: String, field: String, prefix: String,
                      multiWord: Boolean = false): List[Map[String, String]] = {
        val cacheFilter: String => Boolean = str => str(0) == prefix(0)
        var cacheChars: String = prefix(0).toString
        if (prefix.length > 1) {
            cacheChars += prefix(1).toString
        }
        val cacheId: String = cacheIdFormat.format(prefixName, cacheChars)
        getWith(table, field, str => str.startsWith(prefix), cacheFilter, cacheId, multiWord)
    }

    def getWithSuffix(table: String, field: String, suffix: String,
                      multiWord: Boolean = false): List[Map[String, String]] = {
        val cacheFilter: String => Boolean = str => str(str.length - 1) == suffix(suffix.length - 1)

        var cacheChars: String = suffix(suffix.length - 1).toString
        if (suffix.length > 1) {
            cacheChars = suffix(suffix.length - 2).toString + cacheChars
        }
        val cacheId: String = cacheIdFormat.format(suffixName, cacheChars)
        getWith(table, field, str => str.endsWith(suffix), cacheFilter, cacheId, multiWord)
    }

    def getWithRegex(table: String, field: String, regex: String,
                     multiWord: Boolean = false): List[Map[String, String]] = {
        val r: Regex = regex.r
        val cacheId: String = findRegexCacheName(regex)
        val cacheSubstr: String = Utils.getLongestCharSubstring(regex)
        getWith(table, field, str => r.findFirstIn(str) != None, str => str.contains(cacheSubstr),
                cacheId, multiWord)
    }

    def getWithEditDistance(table: String, field: String, target: String,
                            dist: Int, multiWord: Boolean = false): List[Map[String, String]] = {
        val minLength: Int = Utils.max(target.length - dist, 0)
        val maxLength: Int = target.length + dist
        val cacheId: String = cacheIdFormat.format(editDistName, keyFormat.format(minLength.toString, maxLength.toString))
        val cacheFilter: String => Boolean = str => (minLength <= str.length && str.length <= maxLength)
        getWith(table, field, str => Utils.editDistance(str, target, dist), cacheFilter,
                cacheId, multiWord)
    }


    def countWithContains(table: String, field: String, substring: String,
                          multiWord: Boolean = false): Long = {
        val cacheId: String = cacheIdFormat.format(containsName, substring)
        val cacheName: String = createCacheName(table, field, cacheId)
        val fieldPrefix: String = keyFormat.format(field, "")

        statsManager.addRead()

        val cache: Option[String] = cacheManager.get(cacheName)
        if (cache == None) {
            var hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))
            hashRDD = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))

            if (!multiWord) {
                hashRDD = hashRDD.flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
            }

            hashRDD = hashRDD.filter(entry => entry._2.contains(substring))

            if (!multiWord) {
                // Add indices to the cache asychronously
                Future {
                    sparkContext.toRedisSET(hashRDD.map(entry => entry._1.split(":")(1)),
                                            cacheName)
                    cacheManager.add(cacheName, deleteCache)
                }
            }
            hashRDD.count()
        } else {
            val count: Long = redisClient.scard(cache.get).get
            statsManager.addCacheHit(cache.get, table, count.toInt)
            count
        }
    }

    def getWithContains(table: String, field: String, substring: String,
                        multiWord: Boolean = false): List[Map[String, String]] = {
        val cacheId: String = cacheIdFormat.format(containsName, substring)
        val cacheName: String = createCacheName(table, field, cacheId)
        val fieldPrefix: String = keyFormat.format(field, "")

        statsManager.addRead()

        var ids: List[String] = List()

        val cache: Option[String] = cacheManager.get(cacheName)
        if (cache == None) {
            var hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))
            hashRDD = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))

            if (!multiWord) {
                hashRDD = hashRDD.flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
            }

            val idsRDD = hashRDD.filter(entry => entry._2.contains(substring))
                                .map(entry => entry._1.split(":")(1))

            if (!multiWord) {
                // Add indices to the cache asychronously
                Future {
                    sparkContext.toRedisSET(idsRDD, cacheName)
                    cacheManager.add(cacheName, deleteCache)
                }
            }

            ids = idsRDD.collect().toList
        } else {
            ids = redisClient.smembers[String](cache.get).get
                             .map(x => keyFormat.format(table, x.get))
                             .toList
        }

        val queries = ids.map(id => keyFormat.format(table, id))
                         .map(key => (() => redisClient.hgetall[String, String](key)))

        val queryExec = redisClient.pipelineNoMulti(queries)
        queryExec.map(a => Await.result(a.future, timeout))
                 .map(_.asInstanceOf[Option[Map[String, String]]])
                 .map(m => sanitizeData(m.get))
    }

    private def countWith(table: String, field: String, filter: String => Boolean,
                          cacheFilter: String => Boolean, cacheName: String,
                          multiWord: Boolean): Long = {
        val fieldPrefix: String = keyFormat.format(field, "")

        statsManager.addRead()

        var fullCacheName = createCacheName(table, field, cacheName)
        val cache: Option[String] = cacheManager.get(fullCacheName)

        if (cache == None) {
            val hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))

            // We don't cache multiword queries because they are a subset
            // of the total results (for single-word queries)
            if (multiWord) {
                hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                       .filter(entry => filter(entry._2)).count()
            } else {
                var cacheRDD: RDD[(String, String)] = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                cacheRDD = cacheRDD.flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
                                   .filter(entry => cacheFilter(entry._2))
                
                // Add indices to the cache asychronously
                Future {
                    sparkContext.toRedisSET(cacheRDD.map(entry => entry._1.split(":")(1)),
                                                  fullCacheName)
                    cacheManager.add(fullCacheName, deleteCache)
                }

                cacheRDD.filter(entry => filter(entry._2)).count()
            }   
        } else {
            // If the query exactly matches a cache, we can avoid using Spark
            if (fullCacheName == cache.get && fullCacheName.split(":")(2) != editDistName) {
                val count: Long = redisClient.scard(fullCacheName).get
                statsManager.addCacheHit(fullCacheName, table, count.toInt)
                count
            } else {
                fullCacheName = cache.get
                // We fetch the indices into memory as the set of indices is small
                val indices: Array[String] = redisClient.smembers[String](fullCacheName).get
                                                        .map(x => keyFormat.format(table, x.get))
                                                        .toArray

                statsManager.addCacheHit(fullCacheName, table, indices.size)

                val hashRDD = sparkContext.fromRedisHash(indices)
                hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                       .flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
                       .filter(entry => filter(entry._2)).count()
            }
        }
    }

    private def getWith(table: String, field: String, filter: String => Boolean,
                        cacheFilter: String => Boolean, cacheName: String,
                        multiWord: Boolean): List[Map[String, String]] = {
        val fieldPrefix: String = keyFormat.format(field, "")
        var ids: List[String] = List()
        val valueIndex: Int = 1

        statsManager.addRead()

        val cache: Option[String] = cacheManager.get(createCacheName(table, field, cacheName))

        if (cache == None) {
            val fullCacheName = createCacheName(table, field, cacheName)
            val hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))

            if (multiWord) {
                ids = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                             .filter(entry => filter(entry._2))
                             .map(entry => entry._1.split(":")(valueIndex))
                             .collect().toList
            } else {
                var cacheRDD: RDD[(String, String)] = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                cacheRDD = cacheRDD.flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
                                   .filter(entry => cacheFilter(entry._2))

                // Add indices to the cache asychronously
                Future {
                    sparkContext.toRedisSET(cacheRDD.map(entry => entry._1.split(":")(valueIndex)),
                                            fullCacheName)
                    cacheManager.add(fullCacheName, deleteCache)
                }

                ids = cacheRDD.filter(entry => filter(entry._2))
                              .map(entry => entry._1.split(":")(valueIndex))
                              .collect().toList
            }
        } else {
            val fullCacheName = cache.get
            val indices: Array[String] = redisClient.smembers[String](fullCacheName).get
                                                    .map(x => keyFormat.format(table, x.get))
                                                    .toArray

            statsManager.addCacheHit(fullCacheName, table, indices.size)

            val hashRDD = sparkContext.fromRedisHash(indices)
            ids = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                         .flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
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
                              .filter(e => cacheManager.get(e._1) != None)
                              .keys.toList
        val prefixAdds = prefixes.map(name => (() => redisClient.sadd(name, id)))

        val prefixDoubles = newData.filter(e => e._2.length > 1)
                                   .map(e => (e._1, e._2(0).toString + e._2(1).toString))
                                   .map(e => (cacheNameFormat.format(table, e._1, prefixName + ":" + e._2), e._2))
                                   .filter(e => cacheManager.get(e._1) != None)
                                   .keys.toList
        val prefixDoubleAdds = prefixDoubles.map(name => (() => redisClient.sadd(name, id)))

        val suffixes = newData.map(e => (e._1, e._2(e._2.length - 1)))
                              .map(e => (cacheNameFormat.format(table, e._1, suffixName + ":" + e._2), e._2))
                              .filter(e => cacheManager.get(e._1) != None)
                              .keys.toList
        val suffixAdds = suffixes.map(name => (() => redisClient.sadd(name, id)))

        val suffixDoubles = newData.filter(e => e._2.length > 1)
                                   .map(e => (e._1, e._2(e._2.length - 2).toString + e._2(e._2.length - 1).toString))
                                   .map(e => (cacheNameFormat.format(table, e._1, suffixName + ":" + e._2), e._2))
                                   .filter(e => cacheManager.get(e._1) != None)
                                   .keys.toList
        val suffixDoubleAdds = suffixDoubles.map(name => (() => redisClient.sadd(name, id)))

        val containsCaches = new ListBuffer[String]()
        for (field <- fieldsToUpdate) {
            val name: String = cacheNameFormat.format(table, field, containsName)
            val cacheNames: List[String] = cacheManager.getCacheNamesWithPrefix(name)
            for (cacheName <- cacheNames) {
                val tokens: Array[String] = cacheName.split(":")
                if (newData(field).contains(tokens(tokens.length - 1))) {
                    containsCaches += cacheName
                }
            }
        }
        val containsAdds = containsCaches.map(name => (() => redisClient.sadd(name, id)))

        val editDistCaches = new ListBuffer[String]()
        for (field <- fieldsToUpdate) {
            val name: String = cacheNameFormat.format(table, field, editDistName)
            val cacheNames: List[String] = cacheManager.getCacheNamesWithPrefix(name)
            for (cacheName <- cacheNames) {
                val tokens: Array[String] = cacheName.split(":")
                val min: Int = tokens(tokens.length - 2).toInt
                val max: Int = tokens(tokens.length - 1).toInt
                if (newData(field).length >= min && newData(field).length <= max) {
                    editDistCaches += cacheName
                }
            }
        }
        val editDistAdds = editDistCaches.map(name => (() => redisClient.sadd(name, id)))

        val addExec = redisClient.pipelineNoMulti(prefixAdds ++ prefixDoubleAdds ++ suffixAdds ++
                                                  suffixDoubleAdds ++ containsAdds ++ editDistAdds)
        addExec.map(a => Await.result(a.future, timeout * 10))
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
        if (len > 1 && regex(0) == '^' && Utils.isLetter(regex(1))) {
            if (len > 2 && Utils.isLetter(regex(2))) {
                cacheIdFormat.format(prefixName, regex(1).toString + regex(2).toString)
            } else {
                cacheIdFormat.format(prefixName, regex(1).toString)
            }
        } else if (len > 1 && regex(len - 1) == '$' && Utils.isLetter(regex(len - 2))) {
            if (len > 2 && Utils.isLetter(regex(len - 3))) {
                cacheIdFormat.format(suffixName, regex(len - 2).toString + regex(len - 1).toString)
            } else {
                cacheIdFormat.format(suffixName, regex(len - 2).toString)
            }
        } else {
            cacheIdFormat.format(containsName, Utils.getLongestCharSubstring(regex))
        }
    }

    // We choose the first non-stopword to cache
    private def containsCacheName(substring: String): String = {
        val tokens: Array[String] = substring.split(" ")
        for (token <- tokens) {
            if (!stopwords.words.contains(token)) {
                return token
            }
        }
        return substring
    }

}
