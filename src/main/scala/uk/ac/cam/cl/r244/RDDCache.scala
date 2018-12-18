package uk.ac.cam.cl.r244

import scala.collection.mutable.{Map, ListBuffer}
import org.apache.spark.rdd.RDD


class RDDCache(_sizeLimit: Int) {
	val sizeLimit: Int = _sizeLimit
	val cache: Map[String, RDD[(String, String)]] = Map()
	val evictQueue: ListBuffer[String] = ListBuffer()

	def add(key: String, rdd: RDD[(String, String)]): Unit = {
		if (cache.size == sizeLimit) {
			// Evict the Least recently used item
			val keyToEvict: String = evictQueue.remove(0)
			cache(keyToEvict).unpersist()
			cache.remove(keyToEvict)
		}
		evictQueue.append(key)
		rdd.persist()
		cache.update(key, rdd)
	}

	def get(key: String): Option[RDD[(String, String)]] = {
		cache.get(key)
	}

	def contains(key: String): Boolean = {
		cache.contains(key)
	}
}
