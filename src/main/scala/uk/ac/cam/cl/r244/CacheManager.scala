package uk.ac.cam.cl.r244

import java.util.concurrent.ConcurrentHashMap
import collection.JavaConversions._

class CacheManager(_sizeLimit: Int) {
    val sizeLimit: Int = _sizeLimit

    // Used to store the names of cached objects for quick lookup
    // The values are the utility scores of each entry. Since the cache
    // is small, iterating over the entire cache for eviction should be okay
    val cache: ConcurrentHashMap[String, Int] = new ConcurrentHashMap()

    def contains(key: String): Boolean = {
        cache.containsKey(key)
    }

    def getCacheNamesWith(table: String, queryType: String): List[String] = {
        cache.keys.filter(name => {
            val tokens = name.split(":")
            tokens(0) == table && tokens(2) == queryType
        }).toList
    }

    def getCacheNamesWithPrefix(prefix: String): List[String] = {
        cache.keys.filter(name => name.startsWith(prefix)).toList
    }

    def add(key: String, replaceFunc: String => Unit): Unit = {
        if (cache.size == sizeLimit) {
            // Remove the entry with the minimum score
            var minKey = ""
            var minValue: Int = Int.MaxValue
            for (key <- cache.keySet()) {
                if (cache.get(key) < minValue) {
                    minValue = cache.get(key)
                    minKey = key
                }
            }

            cache.remove(minKey)
            replaceFunc(minKey) // This should drop the redis set
        }

        // For now, everything is given a score of zero. This will change with
        // a new replacement policy based on utility and workload.
        cache.put(key, 0)
    }

    // Edit Distance, Smith-Waterman, Ulam Distance

}

