package uk.ac.cam.cl.r244

import spray.json.DefaultJsonProtocol._
import spray.json._ 

final case class BulkWriteResult(result: Long)

object BulkWriteResultProtocol extends DefaultJsonProtocol {
	implicit val bulkWriteResultFormat = jsonFormat1(BulkWriteResult) 
}
