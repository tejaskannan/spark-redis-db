package uk.ac.cam.cl.r244

/**
 * @author tejaskannan
 */

import com.redis.{RedisClient, RedisNode, RedisClientPoolByAddress}
import scala.collection.immutable.{Map, List, Set}
import scala.collection.mutable.ListBuffer
import org.apache.spark.{sql, SparkConf, SparkContext, rdd}, sql.SparkSession, rdd.RDD
import com.redislabs.provider.redis._
import scala.util.{Success, Failure, matching}, matching.Regex
import scala.concurrent.{Future, Promise, Await, duration}, duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.LinkedBlockingQueue

class RedisDatabase(_host: String, _port: Int) {
    val host: String = _host
    val port: Int = _port

    val nodeName = "spark-redis-db"
    val redisNode = new RedisNode(nodeName, host, port)
    val redisClientPool = new RedisClientPoolByAddress(redisNode)
    val statsManager = new StatisticsManager()
    val cacheSize = 64
    val cacheManager = new CacheManager(cacheSize, statsManager)
    val stopwords = new Stopwords()
    var maxCacheThreshold: Double = 0.6


    private val containsCacheThreshold: Int = 4
    private val prefixSuffixThreshold: Int = 3
    private val bulkWriteThreshold: Int = 1000
    private val idField: String = "id"
    private val keyFormat: String = "%s:%s"
    private val tableQueryFormat: String = "%s:*"
    private val t: Int = 10000
    private val timeout: Duration = Duration(t, "millis")
    private val cacheEnabled = true

    val sparkConf = new SparkConf().setMaster("local[4]")
            .setAppName("spark-redis-db")
            .set("spark.redis.host", host)
            .set("spark.redis.port", port.toString)
            .set("spark.redis.auth", "")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    var sparkContext = spark.sparkContext

    val cacheQueue = new LinkedBlockingQueue[CacheQueueEntry]()
    val cacheWriter = new CacheWriter(cacheQueue, cacheManager, deleteCache, host, port)

    // We start the writer in a separate thread
    val cacheWriterThread = new Thread(cacheWriter)
    cacheWriterThread.start()

    def bulkWrite(table: String, records: List[Map[String, String]]): Long = {
        if (table.isEmpty || records.isEmpty) {
            0
        } else {
            // We insert 1000 records at a time
            var count: Long = 0

            var queries = new ListBuffer[Tuple2[String, Map[String, String]]]()
            for (i <- 0 until records.length) {
                val record = records(i)
                if (record.contains(idField)) {
                    val id = record(idField)

                    val key: String = createKey(table, id)
                    val existingRecord: Map[String, String] = get(table, id)
                    updateCaches(table, id, existingRecord, record)

                    statsManager.addWrite()
                    statsManager.addCountToTable(table)

                    queries += new Tuple2(key, prepareData(record, id))

                    if (i % bulkWriteThreshold == 0 || i == records.length - 1) {
                        count += redisClientPool.withClient {
                            client => {
                                val queryExec = client.pipelineNoMulti(queries.map(entry => (() => client.hmset(entry._1, entry._2))))
                                queryExec.map(a => Await.result(a.future, timeout))
                                         .map(_.asInstanceOf[Boolean])
                                         .count(_ == true)
                            }
                        }
                        queries = new ListBuffer[Tuple2[String, Map[String, String]]]()
                    }
                } 
            }
            count
        }
    }

    def write(table: String, id: String, data: Map[String, String]): Boolean = {
        if (table.isEmpty || id.isEmpty || data.isEmpty) {
            false
        } else {
            val key: String = createKey(table, id)

            val existingRecord: Map[String, String] = get(table, id)
            updateCaches(table, id, existingRecord, data)

            statsManager.addWrite()
            statsManager.addCountToTable(table)

            redisClientPool.withClient {
                client => {
                    client.hmset(key, prepareData(data, id))
                }
            }
        } 
    }

    def delete(table: String, id: String): Long = {
        val key: String = createKey(table, id)
        removeIdFromCaches(table, id)
        statsManager.addDelete()
        statsManager.removeCountFromTable(table)

        redisClientPool.withClient {
            client => {
                val result: Option[Long] = client.del(key)
                if (result == None) 0 else result.get
            }
        }
    }

    def deleteCache(cacheName: String): Unit = {
        redisClientPool.withClient {
            client => {
                client.del(cacheName)
            }
        }
    }

    def get(table: String, id: String): Map[String, String] = {
        val key: String = createKey(table, id)
        redisClientPool.withClient {
            client => {
                sanitizeData(client.hgetall[String, String](key).get, List[String]())
            }
        }
    }

    def countWithPrefix(table: String, field: String, prefix: String,
                        multiWord: Boolean = false): Long = {
        var cacheChars: String = prefix
        if (prefix.length > prefixSuffixThreshold) {
            cacheChars = prefix.substring(0, (prefix.length / 2) + 1)
        }
        val cacheFilter: String => Boolean = str => str.length >= cacheChars.length && str.startsWith(cacheChars)
        val cacheName = new CacheName(table, field, QueryTypes.prefixName, List[String](cacheChars))
        val filter: String => Boolean = str => str.startsWith(prefix)
        countWith(table, field, filter, cacheFilter, QueryTypes.prefixName,
                  cacheName, multiWord, prefix == cacheChars)
    }

    def countWithSuffix(table: String, field: String, suffix: String,
                        multiWord: Boolean = false): Long = {
        var cacheChars: String = suffix
        if (suffix.length > prefixSuffixThreshold) {
            cacheChars = suffix.substring(suffix.length / 2)
        }
        val cacheFilter: String => Boolean = str => str.length >= cacheChars.length && str.endsWith(cacheChars)
        val cacheName = new CacheName(table, field, QueryTypes.suffixName, List[String](cacheChars))
        val filter: String => Boolean = str => str.endsWith(suffix)
        countWith(table, field, filter, cacheFilter, QueryTypes.suffixName,
                  cacheName, multiWord, suffix == cacheChars)
    }

    def countWithRegex(table: String, field: String, regex: String,
                       multiWord: Boolean = false): Long = {
        val r: Regex = regex.r
        val cacheSubstr: String = Utils.getLongestCharSubstring(regex)
        val cacheName = new CacheName(table, field, QueryTypes.containsName, List[String](cacheSubstr))
        val cacheFilter: String => Boolean = str => str.length >= cacheSubstr.length && str.contains(cacheSubstr)
        val filter: String => Boolean = str => r.findFirstIn(str) != None
        countWith(table, field, filter, cacheFilter, QueryTypes.containsName,
                  cacheName, multiWord, cacheSubstr == regex)
    }

    def countWithEditDistance(table: String, field: String, target: String,
                              dist: Int, multiWord: Boolean = false): Long = {
        val minLength: Int = Utils.max(target.length - dist, 0)
        val maxLength: Int = target.length + dist
        val cacheName = new CacheName(table, field, QueryTypes.editDistName,
            List[String](minLength.toString, maxLength.toString))
        val cacheFilter: String => Boolean = str => (minLength <= str.length && str.length <= maxLength)
        countWith(table, field, str => Utils.editDistance(str, target, dist), cacheFilter,
                  QueryTypes.editDistName, cacheName, multiWord, false)
    }

    def getWithPrefix(table: String, field: String, prefix: String, multiWord: Boolean,
                      resultFields: List[String]): List[Map[String, String]] = {
        var cacheChars: String = prefix
        if (prefix.length > prefixSuffixThreshold) {
            cacheChars = prefix.substring(0, (prefix.length / 2) + 1)
        }
        val cacheFilter: String => Boolean = str => str.length >= cacheChars.length && str.startsWith(cacheChars)
        val filter: String => Boolean = str => str.startsWith(prefix)
        val cacheName = new CacheName(table, field, QueryTypes.prefixName, List[String](cacheChars))
        getWith(table, field, filter, cacheFilter, QueryTypes.prefixName,
                cacheName, multiWord, prefix == cacheChars, resultFields)
    }

    def getWithSuffix(table: String, field: String, suffix: String, multiWord: Boolean,
                      resultFields: List[String]): List[Map[String, String]] = {
        var cacheChars: String = suffix
        if (suffix.length > prefixSuffixThreshold) {
            cacheChars = suffix.substring(suffix.length / 2)
        }
        val cacheFilter: String => Boolean = str => str.length >= cacheChars.length && str.endsWith(cacheChars)
        val filter: String => Boolean = str => str.endsWith(suffix)
        val cacheName = new CacheName(table, field, QueryTypes.suffixName, List[String](cacheChars))
        getWith(table, field, filter, cacheFilter, QueryTypes.suffixName,
                cacheName, multiWord, suffix == cacheChars, resultFields)
    }

    def getWithRegex(table: String, field: String, regex: String, multiWord: Boolean,
                     resultFields: List[String]): List[Map[String, String]] = {
        val r: Regex = regex.r
        val cacheSubstr: String = Utils.getLongestCharSubstring(regex)
        val cacheName = new CacheName(table, field, QueryTypes.containsName, List[String](cacheSubstr))
        val cacheFilter: String => Boolean = str => str.length >= cacheSubstr.length && str.contains(cacheSubstr)
        val filter: String => Boolean = str => r.findFirstIn(str) != None
        getWith(table, field, filter, cacheFilter, QueryTypes.containsName,
                cacheName, multiWord, regex == cacheSubstr, resultFields)
    }

    def getWithEditDistance(table: String, field: String, target: String, dist: Int,
                            multiWord: Boolean, resultFields: List[String]): List[Map[String, String]] = {
        val minLength: Int = Utils.max(target.length - dist, 0)
        val maxLength: Int = target.length + dist
        val cacheName = new CacheName(table, field, QueryTypes.editDistName,
            List[String](minLength.toString, maxLength.toString))
        val cacheFilter: String => Boolean = str => (minLength <= str.length && str.length <= maxLength)
        getWith(table, field, str => Utils.editDistance(str, target, dist), cacheFilter,
                QueryTypes.editDistName, cacheName, multiWord, false, resultFields)
    }

    def countWithContains(table: String, field: String, substring: String,
                          multiWord: Boolean): Long = {
        var cacheId = containsCacheName(substring)
        if (cacheId.length > containsCacheThreshold) {
            cacheId = cacheId.substring(1, cacheId.length - 1)
        }
        val cacheName = new CacheName(table, field, QueryTypes.containsName, List[String](cacheId))
        val filter: String => Boolean = str => str.contains(substring)
        val cacheFilter: String => Boolean = str => str.length >= cacheId.length && str.contains(cacheId)
        countWith(table, field, filter, cacheFilter, QueryTypes.containsName,
                  cacheName, multiWord, cacheId == substring)
    }

    def getWithContains(table: String, field: String, substring: String,
                        multiWord: Boolean, resultFields: List[String]): List[Map[String, String]] = {
        var cacheId = containsCacheName(substring)
        if (cacheId.length > containsCacheThreshold) {
            cacheId = cacheId.substring(1, cacheId.length - 1)
        }
        val cacheName = new CacheName(table, field, QueryTypes.containsName, List[String](cacheId))
        val filter: String => Boolean = str => str.contains(substring)
        val cacheFilter: String => Boolean = str => str.length >= cacheId.length && str.contains(cacheId)
        getWith(table, field, filter, cacheFilter, QueryTypes.containsName, cacheName,
                multiWord, cacheId == substring, resultFields)
    }

    def countWithSmithWaterman(table: String, field: String, target: String, minScore: Int,
                               multiWord: Boolean): Long = {
        val cacheName = new CacheName(table, field, QueryTypes.swName, List[String](target, minScore.toString))
        val filter: String => Boolean = str => Utils.smithWatermanLinear(str, target, minScore)
        val cacheFilter: String => Boolean = str => true
        countWith(table, field, filter, cacheFilter, QueryTypes.swName,
                  cacheName, multiWord, false)
    }

    def getWithSmithWaterman(table: String, field: String, target: String, minScore: Int,
                             multiWord: Boolean, resultFields: List[String]): List[Map[String, String]] = {
        val cacheName = new CacheName(table, field, QueryTypes.swName, List[String](target, minScore.toString))
        val filter: String => Boolean = str => Utils.smithWatermanLinear(str, target, minScore)
        val cacheFilter: String => Boolean = str => true
        getWith(table, field, filter, cacheFilter, QueryTypes.swName,
                cacheName, multiWord, false, resultFields)
    }

    private def countWith(table: String, field: String, filter: String => Boolean,
                          cacheFilter: String => Boolean, queryType: String, cacheName: CacheName,
                          multiWord: Boolean, cacheOnly: Boolean): Long = {
        val fieldPrefix: String = keyFormat.format(field, "")

        statsManager.addRead()

        val cache: Option[CacheName] = cacheManager.get(cacheName)

        if (cache == None) {
            var hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))
                                      .filter(entry => entry._1.startsWith(fieldPrefix))

            // We don't cache multiword queries because they are a subset
            // of the total results (for single-word queries)
            if (multiWord && queryType != QueryTypes.containsName && queryType != QueryTypes.editDistName) {
                hashRDD.filter(entry => filter(entry._2)).count()
            } else {
                val maxCount = (statsManager.getCountForTable(table) * maxCacheThreshold).toInt

                if (multiWord && (queryType == QueryTypes.containsName || queryType == QueryTypes.editDistName)) {
                    val cacheRDD = hashRDD.filter(entry => cacheFilter(entry._2))
                    cacheRDD.persist()
                    val cacheIds = cacheRDD.map(entry => entry._1.split(":")(1))
                                           .collect().toList

                    if (cacheEnabled && cacheIds.size > 0 && cacheIds.size <= maxCount) {
                        cacheQueue.put(new CacheQueueEntry(cacheIds, cacheName))
                    }

                    val count = cacheRDD.filter(entry => filter(entry._2))
                                        .count()
                    cacheRDD.unpersist()
                    count
                } else {
                    val cacheRDD = hashRDD.flatMap(entry => {
                                        val id = entry._1.split(":")(1)
                                        val words = entry._2.split("\\s+").toSet
                                        words.map(word => (word, id))
                                      })
                                     .groupByKey()
                                     .filter(entry => cacheFilter(entry._1))
                    cacheRDD.persist()
                    val cacheIds = cacheRDD.flatMap(entry => entry._2).collect().toList

                    if (cacheEnabled && cacheIds.size > 0 && cacheIds.size <= maxCount) {
                        cacheQueue.put(new CacheQueueEntry(cacheIds, cacheName))
                    }

                    val count = cacheRDD.filter(entry => filter(entry._1))
                                        .flatMap(entry => entry._2)
                                        .distinct().count()

                    cacheRDD.unpersist()
                    count
                }
            }
        } else {
            val convertedCacheName: CacheName = cache.get
            // If the query exactly matches a cache, we can avoid using Spark
            if (cacheOnly && convertedCacheName == cacheName) {
                val count: Long = redisClientPool.withClient {
                    client => {
                        client.scard(cacheName.toString).get
                    }
                }
                statsManager.addCacheHit(cacheName, count.toInt)
                count  
            } else {
                // We fetch the indices into memory as the set of indices is small
                val indices: Array[String] = redisClientPool.withClient{
                    client => {
                        client.smembers[String](convertedCacheName.toString).get
                              .map(x => keyFormat.format(table, x.get)).toArray
                    }
                }

                statsManager.addCacheHit(convertedCacheName, indices.size)

                val hashRDD = sparkContext.fromRedisHash(indices)
                if (multiWord) {
                    hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                           .filter(entry => filter(entry._2)).count()

                } else {
                    hashRDD.flatMap(entry => {
                            val id = entry._1.split(":")(1)
                            val words = entry._2.split("\\s+").toSet
                            words.map(word => (word, id))
                          })
                         .groupByKey()
                         .filter(entry => filter(entry._1))
                         .flatMap(entry => entry._2)
                         .distinct().count()
                }
            }
        }
    }

    private def getWith(table: String, field: String, filter: String => Boolean,
                        cacheFilter: String => Boolean, queryType: String, cacheName: CacheName,
                        multiWord: Boolean, cacheOnly: Boolean, resultFields: List[String]): List[Map[String, String]] = {
        val fieldPrefix: String = keyFormat.format(field, "")
        var ids: List[String] = List()

        statsManager.addRead()

        val cache: Option[CacheName] = cacheManager.get(cacheName)

        if (cache == None) {
            val hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))
                                      .filter(entry => entry._1.startsWith(fieldPrefix))

            if (multiWord && queryType != QueryTypes.containsName && queryType != QueryTypes.editDistName) {
                ids = hashRDD.filter(entry => filter(entry._2))
                             .map(entry => entry._1.split(":")(1))
                             .collect().toList
            } else {
                val maxCount = (statsManager.getCountForTable(table) * maxCacheThreshold).toInt

                if (multiWord && (queryType == QueryTypes.containsName || queryType == QueryTypes.editDistName)) {
                    val cacheRDD = hashRDD.filter(entry => cacheFilter(entry._2))
                    cacheRDD.persist()
                    val cacheIds = cacheRDD.map(entry => entry._1.split(":")(1))
                                           .collect().toList

                    if (cacheEnabled && cacheIds.size > 0 && cacheIds.size <= maxCount) {
                        cacheQueue.put(new CacheQueueEntry(cacheIds, cacheName))
                    }

                    ids = cacheRDD.filter(entry => filter(entry._2))
                                  .map(entry => entry._1.split(":")(1))
                                  .collect().toList
                    cacheRDD.unpersist()
                } else {
                    val cacheRDD = hashRDD.flatMap(entry => {
                                        val id = entry._1.split(":")(1)
                                        val words = entry._2.split("\\s+").toSet
                                        words.map(word => (word, id))
                                      })
                                     .groupByKey()
                                     .filter(entry => cacheFilter(entry._1))
                    cacheRDD.persist()
                    val cacheIds = cacheRDD.flatMap(entry => entry._2).collect().toList

                    if (cacheEnabled && cacheIds.size > 0 && cacheIds.size <= maxCount) {
                        cacheQueue.put(new CacheQueueEntry(cacheIds, cacheName))
                    }

                    ids = cacheRDD.filter(entry => filter(entry._1))
                                        .flatMap(entry => entry._2)
                                        .distinct().collect().toList

                    cacheRDD.unpersist()
                }
            }
        } else {
            val convertedCacheName: CacheName = cache.get
            val indices: Array[String] = redisClientPool.withClient{
                client => {
                    client.smembers[String](convertedCacheName.toString).get
                          .map(x => keyFormat.format(table, x.get)).toArray
                }
            }

            statsManager.addCacheHit(convertedCacheName, indices.size)

            if (cacheOnly && convertedCacheName == cacheName) {
                ids = indices.toList
            } else {
                val hashRDD = sparkContext.fromRedisHash(indices)
                val fieldRDD = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))

                if (multiWord) {
                    ids = fieldRDD.filter(entry => filter(entry._2))
                                  .map(entry => entry._1.split(":")(1))
                                  .collect().toList
                } else {
                    ids = hashRDD.flatMap(entry => {
                                    val id = entry._1.split(":")(1)
                                    val words = entry._2.split("\\s+").toSet
                                    words.map(word => (word, id))
                                  })
                                 .groupByKey()
                                 .filter(entry => filter(entry._1))
                                 .flatMap(entry => entry._2)
                                 .distinct().collect().toList
                }
            }
        }

        val queries: List[String] = ids.map(id => keyFormat.format(table, id))

        redisClientPool.withClient {
            client => {
                val queryExec = client.pipelineNoMulti(queries.map(key => (() => client.hgetall[String, String](key))))
                queryExec.map(a => Await.result(a.future, timeout))
                    .map(_.asInstanceOf[Option[Map[String, String]]])
                    .map(m => sanitizeData(m.get, resultFields))
            }
        }
    }

    private def removeIdFromCaches(table: String, id: String): Unit = {
        val cacheNames: List[String] = cacheManager.getCacheNamesWithTable(table)
        redisClientPool.withClient {
            client => {
                cacheNames.foreach(name => client.srem(name, id))
            }
        }
    }

    private def updateCaches(table: String, id: String, oldData: Map[String, String],
                             newData: Map[String, String]): List[Long] = {
        val fieldsToUpdate = newData.keys.toList
        for (field <- fieldsToUpdate) {
            val cacheNames: List[String] = cacheManager.getCacheNamesWith(table, field)
            redisClientPool.withClient {
                client => {
                    cacheNames.foreach(name => client.srem(name, id))
                }
            }
        }

        val prefixCaches = new ListBuffer[String]()
        for (field <- fieldsToUpdate) {
            val cacheNames: List[CacheName] = cacheManager.getCacheNameObjectsWith(table, field, QueryTypes.prefixName)
            for (cacheName <- cacheNames) {
                if (newData(field).startsWith(cacheName.getData()(0))) {
                    prefixCaches += cacheName.toString
                }
            }
        }

        val suffixCaches = new ListBuffer[String]()
        for (field <- fieldsToUpdate) {
            val cacheNames: List[CacheName] = cacheManager.getCacheNameObjectsWith(table, field, QueryTypes.suffixName)
            for (cacheName <- cacheNames) {
                if (newData(field).endsWith(cacheName.getData()(0))) {
                    suffixCaches += cacheName.toString
                }
            }
        }

        val containsCaches = new ListBuffer[String]()
        for (field <- fieldsToUpdate) {
            val cacheNames: List[CacheName] = cacheManager.getCacheNameObjectsWith(table, field, QueryTypes.containsName)
            for (cacheName <- cacheNames) {
                if (newData(field).contains(cacheName.getData()(0))) {
                    containsCaches += cacheName.toString
                }
            }
        }

        val editDistCaches = new ListBuffer[String]()
        for (field <- fieldsToUpdate) {
            val cacheNames: List[CacheName] = cacheManager.getCacheNameObjectsWith(table, field, QueryTypes.editDistName)
            for (cacheName <- cacheNames) {
                val min: Int = cacheName.getData()(1).toInt
                val max: Int = cacheName.getData()(0).toInt
                if (newData(field).length >= min && newData(field).length <= max) {
                    editDistCaches += cacheName.toString
                }
            }
        }

        val swCaches = new ListBuffer[String]()
        for (field <- fieldsToUpdate) {
            val cacheNames: List[CacheName] = cacheManager.getCacheNameObjectsWith(table, field, QueryTypes.swName)
            for (cacheName <- cacheNames) {
                val target: String = cacheName.getData()(0)
                val minScore: Int = cacheName.getData()(1).toInt
                if (Utils.smithWatermanLinear(target, newData(field), minScore)) {
                    swCaches += cacheName.toString
                }
            }
        }

        redisClientPool.withClient {
            client => {
                val prefixAdds = prefixCaches.map(name => (() => client.sadd(name, id)))
                val suffixAdds = suffixCaches.map(name => (() => client.sadd(name, id)))
                val containsAdds = containsCaches.map(name => (() => client.sadd(name, id)))
                val editDistAdds = editDistCaches.map(name => (() => client.sadd(name, id)))
                val swAdds = swCaches.map(name => (() => client.sadd(name, id)))

                val addExec = client.pipelineNoMulti(prefixAdds ++ suffixAdds ++ containsAdds ++
                                          editDistAdds ++ swAdds)
                addExec.map(a => Await.result(a.future, timeout * 10))
                       .map(_.asInstanceOf[Option[Long]])
                       .map(_.get)
            }
        }
    }

    private def prepareData(data: Map[String, String], id: String): Map[String, String] = {
        val dataWithId = data + (idField -> id)
        dataWithId.map(entry => (keyFormat.format(entry._1, id), entry._2))
    }

    private def sanitizeData(data: Map[String, String], resultFields: List[String]): Map[String, String] = {
        val sanitized = data.map(entry => (entry._1.split(":")(0), entry._2))
        if (resultFields.isEmpty) {
            sanitized
        } else {
            sanitized.filter(entry => resultFields.contains(entry._1))
        }
    }

    private def createKey(table: String, id: String): String = {
        keyFormat.format(table, id)
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
