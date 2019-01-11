package uk.ac.cam.cl.r244

import spray.json.DefaultJsonProtocol._
import spray.json._

final case class DeleteQuery(table: String, id: String)

object DeleteQueryProtocol extends DefaultJsonProtocol {
    implicit val deleteFormat = jsonFormat2(DeleteQuery)
}
