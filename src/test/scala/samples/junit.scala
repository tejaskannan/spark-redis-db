package samples

import org.junit.{Test, Before}
import org.junit.Assert.{assertTrue, assertFalse, assertEquals}
import uk.ac.cam.cl.r244.RedisDatabase

@Test
class AppTest {

    var db: RedisDatabase = _

    @Before
    def setup(): Unit = {
        val port: Int = 6379
        val host: String = "localhost"
        db = new RedisDatabase(host, port)
    }

    @Test
    def simpleSetGet(): Unit = {
        val table = "nfl"
        val id = "15"
        val data = Map(("firstName" -> "Patrick"), ("lastName" -> "Mahomes"))

        val write: Boolean = db.write(table, id, data)
        assertTrue(write)

        val dataFromDb: Map[String, String] = db.get(table, id)
        assertFalse(dataFromDb.isEmpty)
        mapEquals(data, dataFromDb, id)

        val del: Option[Long] = db.delete(table, id, List[String]())
        assertFalse(del.isEmpty)
        assertEquals(1, del.get)

        val dataFromDbDeleted: Map[String, String] = db.get(table, id)
        assertTrue(dataFromDbDeleted.isEmpty)
        mapEquals(Map[String, String](), dataFromDbDeleted, "")
    }

    def mapEquals(m1: Map[String, String], m2: Map[String, String], id: String): Unit = {
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


