package uk.ac.cam.cl.r244

import org.apache.spark.rdd.RDD

class CacheQueueEntry(_rdd: RDD[(String, String)], _name: CacheName) {
	val rdd = _rdd
	val name = _name
}
