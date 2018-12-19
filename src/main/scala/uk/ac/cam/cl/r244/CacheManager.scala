package uk.ac.cam.cl.r244

import scala.collection.mutable.Map

class CacheManager(_sizeLimit: Int) {
	val sizeLimit: Int = _sizeLimit

	// Used to store the names of cached objects for quick lookup
	// The values are the utility scores of each entry. Since the cache
	// is small, iterating over the entire cache for eviction should be okay
	val cache: Map[String, Int] = Map()

	def contains(key: String): Boolean = {
		cache.contains(key)
	}

	def add(key: String): Unit = {
		if (cache.size == sizeLimit) {
			// Remove the entry with the minimum score
			var minKey: String = cache.minBy(e => e._2)._1
			cache.remove(minKey)
		}

		// For now, everything is given a score of zero. This will change with 
		// a new replacement policy based on utility and workload.
		cache.put(key, 0)
	}

}

