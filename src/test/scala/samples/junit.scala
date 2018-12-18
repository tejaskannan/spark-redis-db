package samples

import org.junit.{Test, Before}
import org.junit.Assert.{assertTrue, assertFalse, assertEquals}
import uk.ac.cam.cl.r244.RedisDatabase

@Test
class AppTest {

    var db: RedisDatabase = _
    val table = "nfl"
    val firstName = "firstName"
    val lastName = "lastName"


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
    }

    @Test
    def sparkCountPrefix(): Unit = {
        val id0 = "15"
        val data0 = Map((firstName -> "Patrick"), (lastName -> "Mahomes"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "18"
        val data1 = Map((firstName -> "Peyton"), (lastName -> "Manning"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val countPrefix0: Long = db.countWithPrefix(table, firstName, "Pat")
        assertEquals(1, countPrefix0)

        val countPrefix1: Long = db.countWithPrefix(table, lastName, "Ma")
        assertEquals(2, countPrefix1)
        
        val countPrefix2: Long = db.countWithPrefix(table, firstName, "A")
        assertEquals(0, countPrefix2)

        db.delete(table, id0, List[String]())
        db.delete(table, id1, List[String]())
    }

    @Test
    def sparkGetPrefix(): Unit = {
        val id0 = "15"
        val data0 = Map((firstName -> "Patrick"), (lastName -> "Mahomes"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "18"
        val data1 = Map((firstName -> "Peyton"), (lastName -> "Manning"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val dataLst: List[Map[String, String]] = List[Map[String, String]](data0, data1)

        val getPrefix0: List[Map[String, String]] = db.getWithPrefix(table, firstName, "Pey")
        assertEquals(1, getPrefix0.size)
        mapEquals(data1, getPrefix0(0))

        val getPrefix1: List[Map[String, String]] = db.getWithPrefix(table, lastName, "Ma")
        listMapEquals(dataLst, getPrefix1)
        
        val getPrefix2: List[Map[String, String]] = db.getWithPrefix(table, firstName, "An")
        assertEquals(0, getPrefix2.size)

        db.delete(table, id0, List[String]())
        db.delete(table, id1, List[String]())
    }

    @Test
    def sparkCountSuffix(): Unit = {
        val id0 = "15"
        val data0 = Map((firstName -> "Patrick"), (lastName -> "Mahomes"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "10"
        val data1 = Map((firstName -> "DeAndre"), (lastName -> "Hopkins"))
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
    }

    @Test
    def sparkGetSuffix(): Unit = {
        val id0 = "15"
        val data0 = Map((firstName -> "Patrick"), (lastName -> "Mahomes"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "10"
        val data1 = Map((firstName -> "DeAndre"), (lastName -> "Hopkins"))
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
    }

    @Test
    def sparkCountRegex(): Unit = {
        val id0 = "14"
        val data0 = Map((firstName -> "Stefon"), (lastName -> "Diggs"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "22"
        val data1 = Map((firstName -> "Stevan"), (lastName -> "Ridley"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val countRegex0: Long = db.countWithRegex(table, lastName, "D.*s")
        assertEquals(1, countRegex0)

        val countRegex1: Long = db.countWithRegex(table, firstName, "Ste.*n")
        assertEquals(2, countRegex1)
        
        val countRegex2: Long = db.countWithRegex(table, firstName, "are")
        assertEquals(0, countRegex2)

        db.delete(table, id0, List[String]())
        db.delete(table, id1, List[String]())
    }

    @Test
    def sparkGetRegex(): Unit = {
        val id0 = "14"
        val data0 = Map((firstName -> "Stefon"), (lastName -> "Diggs"))
        val write0: Boolean = db.write(table, id0, data0)
        assertTrue(write0)

        val id1 = "22"
        val data1 = Map((firstName -> "Stevan"), (lastName -> "Ridley"))
        val write1: Boolean = db.write(table, id1, data1)
        assertTrue(write1)

        val dataLst: List[Map[String, String]] = List[Map[String, String]](data1, data0)

        val getRegex0: List[Map[String, String]] = db.getWithRegex(table, lastName, "D.*s")
        assertEquals(1, getRegex0.size)
        mapEquals(data0, getRegex0(0))

        val getRegex1: List[Map[String, String]] = db.getWithRegex(table, firstName, "Ste.*n")
        listMapEquals(dataLst, getRegex1)
        
        val getRegex2: List[Map[String, String]] = db.getWithRegex(table, firstName, "are")
        assertEquals(0, getRegex2.size)

        db.delete(table, id0, List[String]())
        db.delete(table, id1, List[String]())
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


//    @Test
//    def testKO() = assertTrue(false)

}


