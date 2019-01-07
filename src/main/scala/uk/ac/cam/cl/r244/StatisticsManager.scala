package uk.ac.cam.cl.r244

/**
 * @author tejas.kannan
 */

import scala.collection.mutable.Map
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import collection.JavaConversions._


class StatisticsManager {

    private val numReads: AtomicLong = new AtomicLong(0)
    private val numWrites: AtomicLong = new AtomicLong(0)
    private val numDeletes: AtomicLong = new AtomicLong(0)

    // Tracks total number of comparisons SAVED by cache hits
    private val cacheHits = new ConcurrentHashMap[CacheName, AtomicLong]()

    // Tracks the timestamp (in terms of number of reads) at which each cache was created
    private val cacheAdded = new ConcurrentHashMap[CacheName, Long]()

    // Tracks the total number of records per table
    private val tableCounts = new ConcurrentHashMap[String, AtomicLong]()

    def addCountToTable(table: String): Long = {
        if (!tableCounts.containsKey(table)) {
            tableCounts.put(table, new AtomicLong(0))
        }
        tableCounts.get(table).addAndGet(1)
    }

    def removeCountFromTable(table: String): Long = {
        if (!tableCounts.containsKey(table)) {
            tableCounts.put(table, new AtomicLong(0))
        }
        tableCounts.get(table).addAndGet(-1)
    }

    def getCountForTable(table: String): Long = {
        if (!tableCounts.containsKey(table)) {
            0
        } else {
            tableCounts.get(table).get
        }
    }

    def addCache(cacheName: CacheName): Unit = {
        cacheAdded.put(cacheName, numReads.get)
    }

    def getCacheAdded(cacheName: CacheName): Long = {
        if (!cacheAdded.containsKey(cacheName)) {
            0
        } else {
            cacheAdded.get(cacheName)
        }
    }

    def addCacheHit(cacheName: CacheName, countInCache: Int): Long = {
        if (!cacheHits.containsKey(cacheName)) {
            cacheHits.put(cacheName, new AtomicLong(0))
        }
        val numEntries: Long = getCountForTable(cacheName.getTable)
        cacheHits.get(cacheName).addAndGet(numEntries - countInCache)
    }

    def getNumHits(cacheName: CacheName): Long = {
        if (!cacheHits.containsKey(cacheName)) {
            0
        } else {
            cacheHits.get(cacheName).get
        }
    }

    def removeCache(cacheName: CacheName): Unit = {
        cacheHits.remove(cacheName)
        cacheAdded.remove(cacheName)
    }

    def addRead(): Long = {
        numReads.addAndGet(1)
    }

    def getNumReads(): Long = {
        numReads.get
    }

    def addWrite(): Long = {
        numWrites.addAndGet(1)
    }

    def getNumWrites(): Long = {
        numWrites.get
    }

    def addDelete(): Long = {
        numDeletes.addAndGet(1)
    }

    def getNumDeletes(): Long = {
        numDeletes.get
    }
}
