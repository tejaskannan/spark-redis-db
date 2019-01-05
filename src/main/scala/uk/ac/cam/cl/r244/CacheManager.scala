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
    val cache: ConcurrentHashMap[String, Int] = new ConcurrentHashMap()

    def get(key: String): Option[String] = {
        if (cache.containsKey(key)) {
            Some(key)
        } else {
            val tokens: Array[String] = key.split(splitToken)
            val queryName = tokens(2)

            if (queryName == QueryTypes.editDistName) {
                val min: Int = tokens(3).toInt
                val max: Int = tokens(4).toInt
                val base: String = cacheNameFormat.format(tokens(0), tokens(1), tokens(2))
                for (name <- cache.keys) {
                    if (name.startsWith(base)) {
                        val nameTokens: Array[String] = name.split(splitToken)
                        if (nameTokens(3).toInt <= min && nameTokens(4).toInt >= max) {
                            return Some(name)
                        }
                    }
                }
                None
            } else if (queryName == QueryTypes.swName) {
                val minScore: Int = tokens(tokens.length - 1).toInt
                val base: String = cacheNameFormat.format(tokens(0), tokens(1), tokens(2))
                for (name <- cache.keys) {
                    if (name.startsWith(base)) {
                        val nameTokens: Array[String] = name.split(splitToken)
                        if (nameTokens(nameTokens.length - 1).toInt < minScore) {
                            return Some(name)
                        }
                    }
                }
                None
            } else {
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
