package uk.ac.cam.cl.r244

import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.redislabs.provider.redis._

class CacheWriter(queue: BlockingQueue[CacheQueueEntry], cacheManager: CacheManager,
				  deleteCache: String => Unit, sparkContext: SparkContext) extends Runnable {

	private val running: AtomicBoolean = new AtomicBoolean(true)

	def run() {
		while (running) {
			val cacheEntry: CacheQueueEntry = queue.take()
			val cacheRDD: RDD[(String, String)] = cacheEntry.rdd
			val cacheName: CacheName = cacheEntry.name
			sparkContext.toRedisSET(cacheRDD.map(entry => entry._1.split(":")(1)),
                                    cacheName.toString())
            cacheManager.add(cacheName, deleteCache)
		}
	}

	def shutdown(): Unit = {
		running.set(false)
	}

}