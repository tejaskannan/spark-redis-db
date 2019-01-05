package samples

import org.junit.{Test, Before}
import org.junit.Assert.{assertTrue, assertFalse, assertEquals}
import uk.ac.cam.cl.r244.{RedisDatabase, Utils, StatisticsManager}

@Test
class AppTest {

    var db: RedisDatabase = _
    val table = "nfl"
    val firstName = "firstName"
    val lastName = "lastName"
    val college = "college"
    val cacheFormat = "%s:%s:%s:%s"
    val prefix = "prefix"
    val suffix = "suffix"
    val contains = "contains"
    val editdist = "editdist"

    var statsManager = new StatisticsManager()

    @Before
    def setup(): Unit = {
        val port: Int = 6379
        val host: String = "localhost"
        db = new RedisDatabase(host, port)
        db.cacheManager.setSize(16)
    }

    @Test
    def simpleSetGet(): Unit = {
        val id = "15"
        val data = Map((firstName -> "Patrick"), (lastName -> "Mahomes"))

        val write: Boolean = db.write(table, id, data)
        assertTrue(write)

        val dataFromDb: Map[String, String] = db.get(table, id)
        assertFalse(dataFromDb.isEmpty)
        mapEquals(data, dataFromDb)

        val del: Option[Long] = db.delete(table, id)
        assertFalse(del.isEmpty)
        assertEquals(1, del.get)

        val dataFromDbDeleted: Map[String, String] = db.get(table, id)
        assertTrue(dataFromDbDeleted.isEmpty)
        mapEquals(Map[String, String](), dataFromDbDeleted)

        db.delete(table, id)
    }

    @Test
    def sparkCountPrefix(): Unit = {
        val id0 = "15"
        val data0 = Map((firstName -> "patrick"), (lastName -> "mahomes"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "18"
        val data1 = Map((firstName -> "peyton"), (lastName -> "manning"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val countPrefix0: Long = db.countWithPrefix(table, firstName, "pat")
        assertEquals(1, countPrefix0)

        val countPrefix1: Long = db.countWithPrefix(table, lastName, "ma")
        assertEquals(2, countPrefix1)

        val countPrefix2: Long = db.countWithPrefix(table, firstName, "a")
        assertEquals(0, countPrefix2)

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheFormat.format(table, firstName, prefix, "pa"))
        db.deleteCache(cacheFormat.format(table, lastName, prefix, "ma"))
        db.deleteCache(cacheFormat.format(table, firstName, prefix, "a"))
    }

    @Test
    def sparkGetPrefix(): Unit = {
        val id0 = "15"
        val data0 = Map((firstName -> "petrick"), (lastName -> "mahomes"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "18"
        val data1 = Map((firstName -> "peyton"), (lastName -> "manning"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val dataLst: List[Map[String, String]] = List[Map[String, String]](data0, data1)

        val getPrefix0: List[Map[String, String]] = db.getWithPrefix(table, firstName, "pey")
        assertEquals(1, getPrefix0.size)
        mapEquals(data1, getPrefix0(0))

        // Test the caching
        Thread.sleep(1000)
        val getPrefixCache: List[Map[String, String]] = db.getWithPrefix(table, firstName, "pe")
        listMapEquals(dataLst, getPrefixCache)

        val getPrefix1: List[Map[String, String]] = db.getWithPrefix(table, lastName, "ma")
        listMapEquals(dataLst, getPrefix1)

        val getPrefix2: List[Map[String, String]] = db.getWithPrefix(table, firstName, "an")
        assertEquals(0, getPrefix2.size)

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheFormat.format(table, firstName, prefix, "pe"))
        db.deleteCache(cacheFormat.format(table, lastName, prefix, "ma"))
        db.deleteCache(cacheFormat.format(table, firstName, prefix, "an"))
    }

    @Test
    def sparkCountSuffix(): Unit = {
        val id0 = "15"
        val data0 = Map((firstName -> "patrick"), (lastName -> "mahomes"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "10"
        val data1 = Map((firstName -> "deandre"), (lastName -> "hopkins"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val countSuffix0: Long = db.countWithSuffix(table, firstName, "ck")
        assertEquals(1, countSuffix0)

        val countSuffix1: Long = db.countWithSuffix(table, lastName, "s")
        assertEquals(2, countSuffix1)

        val countSuffix2: Long = db.countWithSuffix(table, firstName, "are")
        assertEquals(0, countSuffix2)

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheFormat.format(table, firstName, suffix, "ck"))
        db.deleteCache(cacheFormat.format(table, lastName, suffix, "s"))
        db.deleteCache(cacheFormat.format(table, firstName, suffix, "re"))
    }

    @Test
    def sparkGetSuffix(): Unit = {
        val id0 = "15"
        val data0 = Map((firstName -> "patrick"), (lastName -> "mahomes"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "10"
        val data1 = Map((firstName -> "deandre"), (lastName -> "hopkins"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val dataLst: List[Map[String, String]] = List[Map[String, String]](data0, data1)

        val getSuffix0: List[Map[String, String]] = db.getWithSuffix(table, lastName, "es")
        assertEquals(1, getSuffix0.size)
        mapEquals(data0, getSuffix0(0))

        val getSuffix1: List[Map[String, String]] = db.getWithSuffix(table, lastName, "s")
        listMapEquals(dataLst, getSuffix1)

        val getSuffix2: List[Map[String, String]] = db.getWithSuffix(table, firstName, "ack")
        assertEquals(0, getSuffix2.size)

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheFormat.format(table, lastName, suffix, "es"))
        db.deleteCache(cacheFormat.format(table, firstName, suffix, "ck"))
    }

    @Test
    def sparkCountRegex(): Unit = {
        val id0 = "14"
        val data0 = Map((firstName -> "stefon"), (lastName -> "diggs"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "22"
        val data1 = Map((firstName -> "stevan"), (lastName -> "ridley"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val countRegex0: Long = db.countWithRegex(table, lastName, ".*d.*s.*")
        assertEquals(1, countRegex0)

        val countRegex1: Long = db.countWithRegex(table, firstName, ".*ste.*n.*")
        assertEquals(2, countRegex1)

        val countRegex2: Long = db.countWithRegex(table, firstName, ".*are.*")
        assertEquals(0, countRegex2)

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheFormat.format(table, lastName, contains, "d"))
        db.deleteCache(cacheFormat.format(table, firstName, contains, "ste"))
        db.deleteCache(cacheFormat.format(table, firstName, contains, "are"))
    }

    @Test
    def sparkGetRegex(): Unit = {
        val id0 = "14"
        val data0 = Map((firstName -> "stefon"), (lastName -> "diggs"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "22"
        val data1 = Map((firstName -> "stevan"), (lastName -> "ridley"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val dataLst: List[Map[String, String]] = List[Map[String, String]](data1, data0)

        val getRegex0: List[Map[String, String]] = db.getWithRegex(table, lastName, ".*d.*s.*")
        assertEquals(1, getRegex0.size)
        mapEquals(data0, getRegex0(0))

        val getRegex1: List[Map[String, String]] = db.getWithRegex(table, firstName, ".*ste.*n.*")
        listMapEquals(dataLst, getRegex1)

        val getRegex2: List[Map[String, String]] = db.getWithRegex(table, firstName, ".*are.*")
        assertEquals(0, getRegex2.size)

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheFormat.format(table, lastName, contains, "d"))
        db.deleteCache(cacheFormat.format(table, firstName, contains, "ste"))
        db.deleteCache(cacheFormat.format(table, firstName, contains, "are"))
    }

    @Test
    def sparkCountContains() {
        val id0 = "28"
        val data0 = Map((firstName -> "james"), (lastName -> "white"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "97"
        val data1 = Map((firstName -> "cameron"), (lastName -> "heyward"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val countContains0: Long = db.countWithContains(table, firstName, "ame", false)
        assertEquals(2, countContains0)

        val countContains1: Long = db.countWithContains(table, lastName, "ite", false)
        assertEquals(1, countContains1)

        val countContains2: Long = db.countWithContains(table, firstName, "hez", false)
        assertEquals(0, countContains2)

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheFormat.format(table, firstName, contains, "ame"))
        db.deleteCache(cacheFormat.format(table, firstName, contains, "hez"))
        db.deleteCache(cacheFormat.format(table, lastName, contains, "ite"))
    }
    
    @Test
    def sparkGetContains() {
        val id0 = "28"
        val data0 = Map((firstName -> "james"), (lastName -> "white"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "97"
        val data1 = Map((firstName -> "cameron"), (lastName -> "heyward"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val dataLst: List[Map[String, String]] = List[Map[String, String]](data0, data1)

        val getContains0: List[Map[String, String]] = db.getWithContains(table, firstName, "ame", false)
        listMapEquals(dataLst, getContains0)

        val getContains1: List[Map[String, String]] = db.getWithContains(table, lastName, "ite", false)
        assertEquals(1, getContains1.size)
        mapEquals(data0, getContains1(0))

        val getContains2: List[Map[String, String]] = db.getWithContains(table, firstName, "hez", false)
        assertTrue(getContains2.isEmpty)

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheFormat.format(table, firstName, contains, "ame"))
        db.deleteCache(cacheFormat.format(table, firstName, contains, "hez"))
        db.deleteCache(cacheFormat.format(table, lastName, contains, "ite"))
    }

    @Test
    def sparkCountWithEditDistance() {
        val id0 = "28"
        val data0 = Map((firstName -> "tavon"), (lastName -> "austin"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "97"
        val data1 = Map((firstName -> "taylor"), (lastName -> "gabriel"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val countContains0: Long = db.countWithEditDistance(table, firstName, "tavlor", 2)
        assertEquals(2, countContains0)

        val countContains1: Long = db.countWithEditDistance(table, firstName, "tavlor", 1)
        assertEquals(1, countContains1)

        val countContains2: Long = db.countWithEditDistance(table, lastName, "zen", 1)
        assertEquals(0, countContains2)

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheFormat.format(table, firstName, editdist, "4:8"))
        db.deleteCache(cacheFormat.format(table, firstName, editdist, "5:7"))
        db.deleteCache(cacheFormat.format(table, lastName, editdist, "2:4"))
    }

    @Test
    def sparkGetWithEditDistance() {
        val id0 = "28"
        val data0 = Map((firstName -> "tavon"), (lastName -> "austin"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "97"
        val data1 = Map((firstName -> "taylor"), (lastName -> "gabriel"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val dataLst: List[Map[String, String]] = List[Map[String, String]](data0, data1)

        val getContains0: List[Map[String, String]] = db.getWithEditDistance(table, firstName, "tavlor", 2)
        listMapEquals(dataLst, getContains0)

        val getContains1: List[Map[String, String]] = db.getWithEditDistance(table, firstName, "tavlor", 1)
        assertEquals(1, getContains1.size)
        mapEquals(data1, getContains1(0))

        val getContains2: List[Map[String, String]] = db.getWithEditDistance(table, lastName, "zen", 1)
        assertTrue(getContains2.isEmpty)

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheFormat.format(table, firstName, editdist, "4:8"))
        db.deleteCache(cacheFormat.format(table, firstName, editdist, "5:7"))
        db.deleteCache(cacheFormat.format(table, lastName, editdist, "2:4"))
    }


    @Test
    def removeFromCache() {
        val id0 = "12"
        val data0 = Map((firstName -> "tom"), (lastName -> "brady"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "30"
        val data1 = Map((firstName -> "todd"), (lastName -> "gurley"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)


        assertEquals(db.countWithPrefix(table, firstName, "to"), 2)

        Thread.sleep(1000)

        val cacheName = cacheFormat.format(table, firstName, prefix, "to")

        val isPresentBefore: Boolean = db.redisClient.sismember(cacheName, id1)
        assertTrue(isPresentBefore)

        db.delete(table, id1)
        Thread.sleep(1000)

        val isPresentAfter: Boolean = db.redisClient.sismember(cacheName, id1)
        assertFalse(isPresentAfter)

        db.delete(table, id0)
        db.deleteCache(cacheName)
    }

    @Test
    def addToCache() {
        val id0 = "1"
        val data0 = Map((firstName -> "cam"), (lastName -> "newton"), (college -> "auburn"))
        assertTrue(db.write(table, id0, data0))

        assertEquals(1, db.countWithPrefix(table, firstName, "ca"))

        Thread.sleep(1000)

        val id1 = "6"
        val data1 = Map((firstName -> "cady"), (lastName -> "kessler"), (college -> "usc"))
        assertTrue(db.write(table, id1, data1))

        val prefixCacheName = cacheFormat.format(table, firstName, prefix, "ca")
        val isPresent0: Boolean = db.redisClient.sismember(prefixCacheName, id1)
        assertTrue(isPresent0)

        assertEquals(1, db.countWithPrefix(table, college, "u"))

        Thread.sleep(1000)

        val id2 = "3"
        val data2 = Map((firstName -> "josh"), (lastName -> "rosen"), (college -> "ucla"))
        assertTrue(db.write(table, id2, data2))

        val collegeCacheName = cacheFormat.format(table, college, prefix, "u")
        val isPresent1: Boolean = db.redisClient.sismember(collegeCacheName, id2)
        assertTrue(isPresent1)

        assertTrue(db.write(table, id0, Map((college -> "uf"))))

        val isPresent2: Boolean = db.redisClient.sismember(collegeCacheName, id0)
        assertTrue(isPresent2)

        db.delete(table, id0)
        db.delete(table, id1)
        db.delete(table, id2)        
        db.deleteCache(prefixCacheName)
        db.deleteCache(collegeCacheName)
    }

    // This test must be run with a cache size of 2
    @Test
    def cacheReplacement() {

        db.cacheManager.setSize(2)

        val id0 = "18" 
        val data0 = Map((firstName -> "peyton"), (lastName -> "manning"), (college -> "tennessee"))

        val id1 = "15"
        val data1 = Map((firstName -> "patrick"), (lastName -> "mahomes"), (college -> "texas tech"))

        val id2 = "17"
        val data2 = Map((firstName -> "philip"), (lastName -> "rivers"), (college -> "nc state"))

        db.write(table, id0, data0)
        db.write(table, id1, data1)
        db.write(table, id2, data2)

        Thread.sleep(1000)

        db.countWithPrefix(table, firstName, "p")
        db.countWithPrefix(table, lastName, "ma")

        Thread.sleep(1000)

        db.countWithRegex(table, firstName, "p.*")
        db.countWithPrefix(table, lastName, "ma")

        val cache0Name = cacheFormat.format(table, firstName, prefix, "p")
        val cache1Name = cacheFormat.format(table, lastName, prefix, "ma")

        assertTrue(db.cacheManager.get(cache0Name) != None)
        assertTrue(db.cacheManager.get(cache1Name) != None)

        assertTrue(db.redisClient.exists(cache0Name))
        assertTrue(db.redisClient.exists(cache1Name))

        db.countWithPrefix(table, college, "te")
        val cache2Name = cacheFormat.format(table, college, prefix, "te")

        Thread.sleep(1000)

        assertTrue(db.cacheManager.get(cache1Name) != None)
        assertTrue(db.cacheManager.get(cache2Name) != None)
        assertTrue(db.cacheManager.get(cache0Name) == None)

        assertTrue(db.redisClient.exists(cache1Name))
        assertTrue(db.redisClient.exists(cache2Name))
        assertFalse(db.redisClient.exists(cache0Name))

        db.delete(table, id0)
        db.delete(table, id1)
        db.delete(table, id2)
        db.deleteCache(cache0Name)
        db.deleteCache(cache2Name)

        db.cacheManager.setSize(16)
    }

    @Test
    def editDistanceCaching() {
        val id0 = "12"
        val data0 = Map((firstName -> "tom"), (lastName -> "brady"))

        val id1 = "10"
        val data1 = Map((firstName -> "tim"), (lastName -> "brown"))

        db.write(table, id0, data0)
        db.write(table, id1, data1)

        assertEquals(2, db.countWithEditDistance(table, firstName, "tamm", 2))

        Thread.sleep(1000)

        assertEquals(2, db.countWithEditDistance(table, firstName, "tam", 1))

        Thread.sleep(1000)

        val cacheName = cacheFormat.format(table, firstName, editdist, "2:6")
        val wrongName = cacheFormat.format(table, firstName, editdist, "2:4")
        assertTrue(db.cacheManager.get(cacheName) != None)
        assertTrue(db.cacheManager.get(wrongName) != None)
        assertEquals(cacheName, db.cacheManager.get(wrongName).get)

        assertTrue(db.redisClient.exists(cacheName))
        assertFalse(db.redisClient.exists(wrongName))

        db.delete(table, id0)
        db.delete(table, id1)
        db.deleteCache(cacheName)
    }

    @Test
    def editDistance() {
        assertTrue(Utils.editDistance("kitten", "sitting", 3))
        assertFalse(Utils.editDistance("kitten", "sitting", 2))
        assertTrue(Utils.editDistance("book", "back", 2))
        assertFalse(Utils.editDistance("book", "back", 1))
        assertTrue(Utils.editDistance("tavon", "tavlor", 2))
        assertFalse(Utils.editDistance("tavon", "tavlor", 1))
        assertTrue(Utils.editDistance("play", "playdate", 4))
        assertFalse(Utils.editDistance("play", "playdate", 3))
    }

    @Test
    def longestSubstring() {
        assertEquals("abc", Utils.getLongestCharSubstring("abc"))
        assertEquals("abc", Utils.getLongestCharSubstring("abc&k"))
        assertEquals("abc", Utils.getLongestCharSubstring("$.*abc[d|e]+^"))
    }

    @Test
    def maxFreqLetter() {
        val maxFreqCutoff = 256
        assertEquals('a', Utils.getMaxFreqLetter("^abc$", statsManager, maxFreqCutoff))
        assertEquals('e', Utils.getMaxFreqLetter("ebai", statsManager, maxFreqCutoff))
        assertEquals('e', Utils.getMaxFreqLetter("^biaze", statsManager, maxFreqCutoff))
        assertEquals('e', Utils.getMaxFreqLetter("biaze$", statsManager, maxFreqCutoff))
        assertEquals('x', Utils.getMaxFreqLetter("xe", statsManager, 1))
    }

    @Test
    def isLetter() {
        assertTrue(Utils.isLetter('a'))
        assertTrue(Utils.isLetter('z'))
        assertFalse(Utils.isLetter('^'))
        assertFalse(Utils.isLetter('.'))
        assertFalse(Utils.isLetter('$'))
    }

    def listMapEquals(m1: List[Map[String, String]], m2: List[Map[String, String]]): Unit = {
        assertTrue(m1.size == m2.size)
        (0 until m1.size).foreach(i => mapEquals(m1(i), m2(i)))
    }

    def mapEquals(m1: Map[String, String], m2: Map[String, String]): Unit = {
        assertTrue(m1.size == m2.size)
        for ((k1, v1) <- m1) {
            assertTrue(m2.contains(k1))
            val v2: String = m2(k1)
            assertEquals(v1, v2)
        }
    }
}
