package uk.ac.cam.cl.r244

import spray.json.DefaultJsonProtocol._
import spray.json._

final case class BulkWriteQuery(table: String, records: List[Map[String, String]])

object BulkWriteQueryProtocol extends DefaultJsonProtocol {
    implicit val bulkWriteFormat = jsonFormat2(BulkWriteQuery)
}
