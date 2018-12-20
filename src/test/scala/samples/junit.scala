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
    val cacheFormat = "%s:%s:%s:%s"
    val prefix = "prefix"
    val suffix = "suffix"
    val contains = "contains"

    var statsManager = new StatisticsManager()

    @Before
    def setup(): Unit = {
        val port: Int = 6379
        val host: String = "localhost"
        db = new RedisDatabase(host, port)
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

        val del: Option[Long] = db.delete(table, id, List[String]())
        assertFalse(del.isEmpty)
        assertEquals(1, del.get)

        val dataFromDbDeleted: Map[String, String] = db.get(table, id)
        assertTrue(dataFromDbDeleted.isEmpty)
        mapEquals(Map[String, String](), dataFromDbDeleted)

        db.delete(table, id, List[String]())
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

        db.delete(table, id0, List[String]())
        db.delete(table, id1, List[String]())
        db.deleteTable(cacheFormat.format(table, firstName, prefix, "p"))
        db.deleteTable(cacheFormat.format(table, lastName, prefix, "m"))
        db.deleteTable(cacheFormat.format(table, firstName, prefix, "a"))
    }

    @Test
    def sparkGetPrefix(): Unit = {
        val id0 = "15"
        val data0 = Map((firstName -> "patrick"), (lastName -> "mahomes"))
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
        val getPrefixCache: List[Map[String, String]] = db.getWithPrefix(table, firstName, "p")
        listMapEquals(dataLst, getPrefixCache)

        val getPrefix1: List[Map[String, String]] = db.getWithPrefix(table, lastName, "ma")
        listMapEquals(dataLst, getPrefix1)

        val getPrefix2: List[Map[String, String]] = db.getWithPrefix(table, firstName, "an")
        assertEquals(0, getPrefix2.size)

        db.delete(table, id0, List[String]())
        db.delete(table, id1, List[String]())
        db.deleteTable(cacheFormat.format(table, firstName, prefix, "p"))
        db.deleteTable(cacheFormat.format(table, lastName, prefix, "m"))
        db.deleteTable(cacheFormat.format(table, firstName, prefix, "a"))
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

        db.delete(table, id0, List[String]())
        db.delete(table, id1, List[String]())
        db.deleteTable(cacheFormat.format(table, firstName, suffix, "k"))
        db.deleteTable(cacheFormat.format(table, lastName, suffix, "s"))
        db.deleteTable(cacheFormat.format(table, firstName, suffix, "e"))
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

        db.delete(table, id0, List[String]())
        db.delete(table, id1, List[String]())
        db.deleteTable(cacheFormat.format(table, lastName, suffix, "s"))
        db.deleteTable(cacheFormat.format(table, firstName, suffix, "k"))
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

        db.delete(table, id0, List[String]())
        db.delete(table, id1, List[String]())
        db.deleteTable(cacheFormat.format(table, lastName, contains, "s"))
        db.deleteTable(cacheFormat.format(table, firstName, contains, "e"))
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

        db.delete(table, id0, List[String]())
        db.delete(table, id1, List[String]())
        db.deleteTable(cacheFormat.format(table, lastName, contains, "s"))
        db.deleteTable(cacheFormat.format(table, firstName, contains, "e"))
    }

    @Test
    def editDistance() {
        assertTrue(Utils.editDistance("kitten", "sitting", 3))
        assertFalse(Utils.editDistance("kitten", "sitting", 2))
        assertTrue(Utils.editDistance("book", "back", 2))
        assertFalse(Utils.editDistance("book", "back", 1))
    }

    @Test
    def maxFreqRegex() {
        assertEquals('b', Utils.getMaxFreqLetter("^abc$", statsManager))
        assertEquals('a', Utils.getMaxFreqLetter("ebai", statsManager))
        assertEquals('i', Utils.getMaxFreqLetter("^biaze", statsManager))
        assertEquals('i', Utils.getMaxFreqLetter("biaze$", statsManager))
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
