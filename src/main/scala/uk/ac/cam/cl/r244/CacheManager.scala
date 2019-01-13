package uk.ac.cam.cl.r244

import java.util.concurrent.ConcurrentHashMap
import collection.JavaConversions._

class CacheManager(_sizeLimit: Int, statsManager: StatisticsManager) {
    private var sizeLimit: Int = _sizeLimit
    private val splitToken: String = ":"
    private val cacheNameFormat: String = "%s:%s:%s"

    // Used to store the names of cached objects for quick lookup
    // The values are the utility scores of each entry. Since the cache
    // is small, iterating over the entire cache for eviction should be okay
    var cache: ConcurrentHashMap[CacheName, Byte] = new ConcurrentHashMap()

    def flush(): Unit = {
        cache = new ConcurrentHashMap()
        statsManager.reset()
    }

    def get(key: CacheName): Option[CacheName] = {
        if (cache.containsKey(key)) {
            Some(key)
        } else {
            val queryType = key.getQueryType()
            val table = key.getTable()
            val field = key.getField()

            if (queryType == QueryTypes.editDistName) {
                val min: Int = key.getData()(0).toInt
                val max: Int = key.getData()(1).toInt
                for (name <- cache.keys) {
                    if (name.getTable() == table && name.getField() == field &&
                        name.getQueryType() == queryType) {
                        if (name.getData()(0).toInt <= min && name.getData()(1).toInt >= max) {
                            return Some(name)
                        }
                    }
                }
                None
            } else if (queryType == QueryTypes.swName) {
                val minScore: Int = key.getData()(1).toInt
                for (name <- cache.keys) {
                    if (name.getTable() == table && name.getField() == field &&
                        name.getQueryType() == queryType) {
                        if (name.getData()(1).toInt < minScore) {
                            return Some(name)
                        }
                    }
                }
                None
            } else {
                val substr: String = key.getData()(0)
                for (name <- cache.keys) {
                    if (name.getTable() == table && name.getField() == field &&
                        name.getQueryType() == QueryTypes.containsName) {
                        if (name.getData()(0).contains(substr)) {
                            return Some(name)
                        }
                    }
                }
                None
            }
        }
    }

    def setSize(newSize: Int): Unit = {
        sizeLimit = newSize
    }

    def getSize(): Int = {
        sizeLimit
    }

    def getCacheNamesWith(table: String, field: String): List[String] = {
        cache.keys.filter(name => {
            name.getTable() == table && name.getField() == field
        }).map(name => name.toString()).toList
    }

    def getCacheNamesWithTable(table: String): List[String] = {
        cache.keys.filter(name => name.getTable == table)
                  .map(name => name.toString()).toList
    }

    def getCacheNameObjectsWith(table: String, field: String, queryType: String): List[CacheName] = {
        cache.keys.filter(name => {
            name.getTable() == table && name.getQueryType() == queryType && name.getField() == field
        }).toList
    }

    def getCacheScore(cacheName: CacheName): Double = {
        val age: Long = statsManager.getNumReads - statsManager.getCacheAdded(cacheName)
        val hits: Long = statsManager.getNumHits(cacheName)
        hits.toDouble / age.toDouble
    }

    def add(key: CacheName, replaceFunc: String => Unit): Unit = {
        if (cache.size == sizeLimit) {
            // Remove the entry with the minimum score
            var minKey: CacheName = null
            var minValue: Double = Double.MaxValue
            for (key <- cache.keySet()) {
                val score: Double = getCacheScore(key)
                if (score < minValue) {
                    minValue = score
                    minKey = key
                }
            }

            cache.remove(minKey)
            statsManager.removeCache(minKey)
            replaceFunc(minKey.toString()) // This should drop the redis set
        }

        cache.put(key, 0)
        statsManager.addCache(key)
    }
}
