package uk.ac.cam.cl.r244

import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark.SparkContext
import com.redis.RedisClient
import org.apache.spark.rdd.RDD
import com.redislabs.provider.redis._

class CacheWriter(queue: BlockingQueue[CacheQueueEntry], cacheManager: CacheManager,
				  deleteCache: String => Unit, host: String, port: Int) extends Runnable {

	private val running: AtomicBoolean = new AtomicBoolean(true)

	// We can keep a single client because we create caches in a synchronous manner
	private val redisClient = new RedisClient(host, port)

	def run() {
		while (running.get()) {
			val cacheEntry: CacheQueueEntry = queue.take()
			if (cacheEntry.ids.size > 0) {
				val ids = cacheEntry.ids.map(id => id.toString)
				redisClient.sadd(cacheEntry.name, ids(0), ids:_*)
				cacheManager.add(cacheEntry.name, deleteCache)
			}
		}
	}

	def shutdown(): Unit = {
		running.set(false)
	}

}