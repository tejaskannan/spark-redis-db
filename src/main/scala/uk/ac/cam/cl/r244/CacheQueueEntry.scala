package uk.ac.cam.cl.r244

// import org.apache.spark.rdd.RDD
import scala.collection.immutable.List

class CacheQueueEntry(_ids: List[String], _name: CacheName) {
	val ids = _ids
	val name = _name
}
