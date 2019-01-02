package uk.ac.cam.cl.r244

import java.util.concurrent.ConcurrentHashMap
import collection.JavaConversions._

class CacheManager(_sizeLimit: Int, statsManager: StatisticsManager) {
    private var sizeLimit: Int = _sizeLimit
    private val splitToken: String = ":"

    // Used to store the names of cached objects for quick lookup
    // The values are the utility scores of each entry. Since the cache
    // is small, iterating over the entire cache for eviction should be okay
    val cache: ConcurrentHashMap[String, Int] = new ConcurrentHashMap()

    def contains(key: String): Boolean = {
        cache.containsKey(key)
    }

    def setSize(newSize: Int): Unit = {
        sizeLimit = newSize
    }

    def getSize(): Int = {
        sizeLimit
    }

    def getCacheNamesWith(table: String, queryType: String): List[String] = {
        cache.keys.filter(name => {
            val tokens = name.split(splitToken)
            tokens(0) == table && tokens(2) == queryType
        }).toList
    }

    def getCacheNamesWithPrefix(prefix: String): List[String] = {
        cache.keys.filter(name => name.startsWith(prefix)).toList
    }

    def getCacheScore(cacheName: String): Double = {
        val age: Int = statsManager.getNumReads - statsManager.getCacheAdded(cacheName)
        val hits: Int = statsManager.getNumHits(cacheName)
        hits.toDouble / age.toDouble
    }

    def add(key: String, replaceFunc: String => Unit): Unit = {
        if (cache.size == sizeLimit) {
            // Remove the entry with the minimum score
            var minKey = ""
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
            replaceFunc(minKey) // This should drop the redis set
        }

        cache.put(key, 0)
        statsManager.addCache(key)
    }
}
