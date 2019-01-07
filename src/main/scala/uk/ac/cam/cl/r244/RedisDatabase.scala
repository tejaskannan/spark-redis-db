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

    private val idField: String = "id"
    private val keyFormat: String = "%s:%s"
    private val tableQueryFormat: String = "%s:*"
    private val t: Int = 10000
    private val timeout: Duration = Duration(t, "millis")

    val sparkConf = new SparkConf().setMaster("local[4]")
            .setAppName("spark-redis-db")
            .set("spark.redis.host", host)
            .set("spark.redis.port", port.toString)
            .set("spark.redis.auth", "")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    var sparkContext = spark.sparkContext

    def bulkWrite(table: String, records: List[Map[String, String]]): Long = {
        if (table.isEmpty || records.isEmpty) {
            0
        } else {
            // We insert 1000 records at a time
            val threshold = 1000
            var count: Long = 0

            var queries = new ListBuffer[() => Boolean]()
            for (i <- 0 until records.length) {
                val record = records(i)
                if (record.contains(idField)) {
                    val id = record(idField)

                    val key: String = createKey(table, id)
                    val existingRecord: Map[String, String] = get(table, id)
                    updateCaches(table, id, existingRecord, record)

                    statsManager.addWrite()
                    statsManager.addCountToTable(table)

                    queries += (() => redisClient.hmset(key, prepareData(record, id)))

                    if (i % threshold == 0 || i == records.length - 1) {
                        val queryExec = redisClient.pipelineNoMulti(queries)
                        count += queryExec.map(a => Await.result(a.future, timeout))
                                          .map(_.asInstanceOf[Boolean])
                                          .count(_ == true)
                        queries = new ListBuffer[() => Boolean]()
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

            redisClient.hmset(key, prepareData(data, id))
        } 
    }

    def delete(table: String, id: String): Long = {
        val key: String = createKey(table, id)
        removeIdFromCaches(table, id)
        statsManager.addDelete()
        statsManager.removeCountFromTable(table)
        val result: Option[Long] = redisClient.del(key)
        if (result == None) 0 else result.get
    }

    def deleteCache(cacheName: String): Unit = {
        redisClient.del(cacheName)
    }

    def get(table: String, id: String): Map[String, String] = {
        val key: String = createKey(table, id)
        sanitizeData(redisClient.hgetall[String, String](key).get, List[String]())
    }

    def countWithPrefix(table: String, field: String, prefix: String,
                        multiWord: Boolean = false): Long = {
        var cacheChars: String = prefix(0).toString
        if (prefix.length > 1) {
            cacheChars += prefix(1).toString
        }
        val cacheFilter: String => Boolean = str => str.startsWith(cacheChars)
        val cacheName = new CacheName(table, field, QueryTypes.prefixName, List[String](cacheChars))
        countWith(table, field, str => str.startsWith(prefix), cacheFilter,
                  cacheName, multiWord, prefix.length <= 2)
    }

    def countWithSuffix(table: String, field: String, suffix: String,
                        multiWord: Boolean = false): Long = {
        var cacheChars: String = suffix(suffix.length - 1).toString
        if (suffix.length > 1) {
            cacheChars = suffix(suffix.length - 2).toString + cacheChars
        }
        val cacheFilter: String => Boolean = str => str.endsWith(cacheChars)
        val cacheName = new CacheName(table, field, QueryTypes.suffixName, List[String](cacheChars))
        countWith(table, field, str => str.endsWith(suffix), cacheFilter,
                  cacheName, multiWord, suffix.length <= 2)
    }

    def countWithRegex(table: String, field: String, regex: String,
                       multiWord: Boolean = false): Long = {
        val r: Regex = regex.r
        val cacheSubstr: String = Utils.getLongestCharSubstring(regex)
        val cacheName = new CacheName(table, field, QueryTypes.containsName, List[String](cacheSubstr))
        countWith(table, field, str => r.findFirstIn(str) != None, str => str.contains(cacheSubstr),
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
                  cacheName, multiWord, false)
    }

    def getWithPrefix(table: String, field: String, prefix: String, multiWord: Boolean,
                      resultFields: List[String]): List[Map[String, String]] = {
        var cacheChars: String = prefix(0).toString
        if (prefix.length > 1) {
            cacheChars += prefix(1).toString
        }
        val cacheFilter: String => Boolean = str => str.startsWith(cacheChars)
        val cacheName = new CacheName(table, field, QueryTypes.prefixName, List[String](cacheChars))
        getWith(table, field, str => str.startsWith(prefix), cacheFilter, cacheName,
                multiWord, resultFields)
    }

    def getWithSuffix(table: String, field: String, suffix: String, multiWord: Boolean,
                      resultFields: List[String]): List[Map[String, String]] = {
        var cacheChars: String = suffix(suffix.length - 1).toString
        if (suffix.length > 1) {
            cacheChars = suffix(suffix.length - 2).toString + cacheChars
        }
        val cacheFilter: String => Boolean = str => str.endsWith(cacheChars)
        val cacheName = new CacheName(table, field, QueryTypes.suffixName, List[String](cacheChars))
        getWith(table, field, str => str.endsWith(suffix), cacheFilter, cacheName,
                multiWord, resultFields)
    }

    def getWithRegex(table: String, field: String, regex: String, multiWord: Boolean,
                     resultFields: List[String]): List[Map[String, String]] = {
        val r: Regex = regex.r
        val cacheSubstr: String = Utils.getLongestCharSubstring(regex)
        val cacheName = new CacheName(table, field, QueryTypes.containsName, List[String](cacheSubstr))
        getWith(table, field, str => r.findFirstIn(str) != None, str => str.contains(cacheSubstr),
                cacheName, multiWord, resultFields)
    }

    def getWithEditDistance(table: String, field: String, target: String, dist: Int,
                            multiWord: Boolean, resultFields: List[String]): List[Map[String, String]] = {
        val minLength: Int = Utils.max(target.length - dist, 0)
        val maxLength: Int = target.length + dist
        val cacheName = new CacheName(table, field, QueryTypes.editDistName,
            List[String](minLength.toString, maxLength.toString))
        val cacheFilter: String => Boolean = str => (minLength <= str.length && str.length <= maxLength)
        getWith(table, field, str => Utils.editDistance(str, target, dist), cacheFilter,
                cacheName, multiWord, resultFields)
    }

    def countWithContains(table: String, field: String, substring: String,
                          multiWord: Boolean): Long = {
        val cacheName = new CacheName(table, field, QueryTypes.containsName, List[String](substring))
        val filter: String => Boolean = str => str.contains(substring)
        countWithExact(table, field, filter, cacheName, multiWord)
    }

    def getWithContains(table: String, field: String, substring: String,
                        multiWord: Boolean, resultFields: List[String]): List[Map[String, String]] = {
        val cacheName = new CacheName(table, field, QueryTypes.containsName, List[String](substring))
        val filter: String => Boolean = str => str.contains(substring)
        getWithExact(table, field, filter, cacheName, multiWord, resultFields)
    }

    def countWithSmithWaterman(table: String, field: String, target: String, minScore: Int,
                               multiWord: Boolean): Long = {
        val cacheName = new CacheName(table, field, QueryTypes.swName, List[String](target, minScore.toString))
        val filter: String => Boolean = str => Utils.smithWatermanLinear(str, target, minScore)
        countWithExact(table, field, filter, cacheName, multiWord)
    }

    def getWithSmithWaterman(table: String, field: String, target: String, minScore: Int,
                             multiWord: Boolean, resultFields: List[String]): List[Map[String, String]] = {
        val cacheName = new CacheName(table, field, QueryTypes.swName, List[String](target, minScore.toString))
        val filter: String => Boolean = str => Utils.smithWatermanLinear(str, target, minScore)
        getWithExact(table, field, filter, cacheName, multiWord, resultFields)
    }


    private def countWithExact(table: String, field: String, filter: String => Boolean,
                               cacheName: CacheName, multiWord: Boolean): Long = {
        val fieldPrefix: String = keyFormat.format(field, "")

        statsManager.addRead()

        val cache: Option[CacheName] = cacheManager.get(cacheName)
        if (cache == None) {
            var hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))
            hashRDD = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))

            if (!multiWord) {
                hashRDD = hashRDD.flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
            }

            hashRDD = hashRDD.filter(entry => filter(entry._2))

            val count = hashRDD.count()

            if (!multiWord && count > 0) {
                // Add indices to the cache asychronously
                Future {
                    sparkContext.toRedisSET(hashRDD.map(entry => entry._1.split(":")(1)),
                                            cacheName.toString)
                    cacheManager.add(cacheName, deleteCache)
                }
            }
            count
        } else {
            if (cacheName == cache.get) {
                val count: Long = redisClient.scard(cacheName.toString).get
                statsManager.addCacheHit(cacheName, count.toInt)
                count
            } else {
                // We are in a case where a smith-waterman score is less than the current
                // query, so we can reuse those results here
                // We fetch the indices into memory as the set of indices is small
                val indices: Array[String] = redisClient.smembers[String](cacheName.toString).get
                                                        .map(x => keyFormat.format(table, x.get))
                                                        .toArray

                statsManager.addCacheHit(cacheName, indices.size)

                val hashRDD = sparkContext.fromRedisHash(indices)
                hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                       .flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
                       .filter(entry => filter(entry._2)).count()
            }

        }
    }

    private def getWithExact(table: String, field: String, filter: String => Boolean,
                             cacheName: CacheName, multiWord: Boolean,
                             resultFields: List[String]): List[Map[String, String]] = {
        val fieldPrefix: String = keyFormat.format(field, "")

        statsManager.addRead()

        var ids: List[String] = List()

        val cache: Option[CacheName] = cacheManager.get(cacheName)
        if (cache == None) {
            var hashRDD = sparkContext.fromRedisHash(tableQueryFormat.format(table))
            hashRDD = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))

            if (!multiWord) {
                hashRDD = hashRDD.flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
            }

            val idsRDD = hashRDD.filter(entry => filter(entry._2))
                                .map(entry => entry._1.split(":")(1))

            ids = idsRDD.collect().toList

            if (!multiWord && ids.size > 0) {
                // Add indices to the cache asychronously
                Future {
                    sparkContext.toRedisSET(idsRDD, cacheName.toString)
                    cacheManager.add(cacheName, deleteCache)
                }
            }

        } else {
            if (cacheName == cache.get) {
               ids = redisClient.smembers[String](cacheName.toString).get
                             .map(x => x.get)
                             .toList
            } else {
                // We are in a case where there is a cache for Smith-Waterman with a lower
                // minimum score than the one currently being queried
                val indices: Array[String] = redisClient.smembers[String](cacheName.toString).get
                                                        .map(x => keyFormat.format(table, x.get))
                                                        .toArray

                statsManager.addCacheHit(cacheName, indices.size)

                val hashRDD = sparkContext.fromRedisHash(indices)
                ids = hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                             .flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
                             .filter(entry => filter(entry._2))
                             .map(entry => entry._1.split(":")(1))
                             .collect().toList
            }

        }

        val queries = ids.map(id => keyFormat.format(table, id))
                         .map(key => (() => redisClient.hgetall[String, String](key)))

        val queryExec = redisClient.pipelineNoMulti(queries)
        queryExec.map(a => Await.result(a.future, timeout))
                 .map(_.asInstanceOf[Option[Map[String, String]]])
                 .map(m => sanitizeData(m.get, resultFields))
    }

    private def countWith(table: String, field: String, filter: String => Boolean,
                          cacheFilter: String => Boolean, cacheName: CacheName,
                          multiWord: Boolean, cacheOnly: Boolean): Long = {
        val fieldPrefix: String = keyFormat.format(field, "")

        statsManager.addRead()

        val cache: Option[CacheName] = cacheManager.get(cacheName)

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

                val count = cacheRDD.filter(entry => filter(entry._2)).count()

                if (count > 0) {
                    // Add indices to the cache asychronously
                    Future {
                        sparkContext.toRedisSET(cacheRDD.map(entry => entry._1.split(":")(1)),
                                                cacheName.toString())
                        cacheManager.add(cacheName, deleteCache)
                    }
                }

                count
            }
        } else {
            // If the query exactly matches a cache, we can avoid using Spark
            if (cacheOnly) {
                val count: Long = redisClient.scard(cacheName.toString).get
                statsManager.addCacheHit(cacheName, count.toInt)
                count
            } else {
                // We fetch the indices into memory as the set of indices is small
                val indices: Array[String] = redisClient.smembers[String](cacheName.toString).get
                                                        .map(x => keyFormat.format(table, x.get))
                                                        .toArray

                statsManager.addCacheHit(cacheName, indices.size)

                val hashRDD = sparkContext.fromRedisHash(indices)
                hashRDD.filter(entry => entry._1.startsWith(fieldPrefix))
                       .flatMap(entry => entry._2.split("\\s+").map(word => (entry._1, word)).toSet.toList)
                       .filter(entry => filter(entry._2)).count()
            }
        }
    }

    private def getWith(table: String, field: String, filter: String => Boolean,
                        cacheFilter: String => Boolean, cacheName: CacheName,
                        multiWord: Boolean, resultFields: List[String]): List[Map[String, String]] = {
        val fieldPrefix: String = keyFormat.format(field, "")
        var ids: List[String] = List()
        val valueIndex: Int = 1

        statsManager.addRead()

        val cache: Option[CacheName] = cacheManager.get(cacheName)

        if (cache == None) {
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

                ids = cacheRDD.filter(entry => filter(entry._2))
                              .map(entry => entry._1.split(":")(valueIndex))
                              .collect().toList

                if (ids.size > 0) {
                    // Add indices to the cache asychronously
                    Future {
                        sparkContext.toRedisSET(cacheRDD.map(entry => entry._1.split(":")(valueIndex)),
                                                cacheName.toString)
                        cacheManager.add(cacheName, deleteCache)
                    }
                }
            }
        } else {
            val indices: Array[String] = redisClient.smembers[String](cacheName.toString).get
                                                    .map(x => keyFormat.format(table, x.get))
                                                    .toArray

            statsManager.addCacheHit(cacheName, indices.size)

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
                 .map(m => sanitizeData(m.get, resultFields))
    }

    private def removeIdFromCaches(table: String, id: String): Unit = {
        val cacheNames: List[String] = cacheManager.getCacheNamesWithTable(table)
        cacheNames.foreach(name => redisClient.srem(name, id))
    }

    private def updateCaches(table: String, id: String, oldData: Map[String, String],
                             newData: Map[String, String]): List[Long] = {
        val fieldsToUpdate = newData.keys.toList
        for (field <- fieldsToUpdate) {
            val cacheNames: List[String] = cacheManager.getCacheNamesWith(table, field)
            cacheNames.foreach(name => redisClient.srem(name, id))
        }

        val prefixes = newData.map(e => (e._1, e._2(0)))
                .map(e => (new CacheName(table, e._1, QueryTypes.prefixName, List[String](e._2.toString))))
                .filter(e => cacheManager.get(e) != None)
                .map(e => e.toString)
                .toList
        val prefixAdds = prefixes.map(name => (() => redisClient.sadd(name, id)))

        val prefixDoubles = newData.filter(e => e._2.length > 1)
                .map(e => (e._1, e._2(0).toString + e._2(1).toString))
                .map(e => (new CacheName(table, e._1, QueryTypes.prefixName, List[String](e._2))))
                .filter(e => cacheManager.get(e) != None)
                .map(e => e.toString)
                .toList
        val prefixDoubleAdds = prefixDoubles.map(name => (() => redisClient.sadd(name, id)))

        val suffixes = newData.map(e => (e._1, e._2(e._2.length - 1)))
                .map(e => (new CacheName(table, e._1, QueryTypes.suffixName, List[String](e._2.toString))))
                .filter(e => cacheManager.get(e) != None)
                .map(e => e.toString)
                .toList
        val suffixAdds = suffixes.map(name => (() => redisClient.sadd(name, id)))

        val suffixDoubles = newData.filter(e => e._2.length > 1)
                .map(e => (e._1, e._2(e._2.length - 2).toString + e._2(e._2.length - 1).toString))
                .map(e => (new CacheName(table, e._1, QueryTypes.suffixName, List[String](e._2))))
                .filter(e => cacheManager.get(e) != None)
                .map(e => e.toString)
                .toList
        val suffixDoubleAdds = suffixDoubles.map(name => (() => redisClient.sadd(name, id)))

        val containsCaches = new ListBuffer[String]()
        for (field <- fieldsToUpdate) {
            val cacheNames: List[CacheName] = cacheManager.getCacheNameObjectsWith(table, field, QueryTypes.containsName)
            for (cacheName <- cacheNames) {
                if (newData(field).contains(cacheName.getData()(0))) {
                    containsCaches += cacheName.toString
                }
            }
        }
        val containsAdds = containsCaches.map(name => (() => redisClient.sadd(name, id)))

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
        val editDistAdds = editDistCaches.map(name => (() => redisClient.sadd(name, id)))

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
        val swAdds = swCaches.map(name => (() => redisClient.sadd(name, id)))

        val addExec = redisClient.pipelineNoMulti(prefixAdds ++ prefixDoubleAdds ++ suffixAdds ++
                                                  suffixDoubleAdds ++ containsAdds ++ editDistAdds ++
                                                  swAdds)
        addExec.map(a => Await.result(a.future, timeout * 10))
               .map(_.asInstanceOf[Option[Long]])
               .map(_.get)
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
