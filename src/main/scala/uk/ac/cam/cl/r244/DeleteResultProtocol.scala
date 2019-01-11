package uk.ac.cam.cl.r244

import spray.json.DefaultJsonProtocol._
import spray.json._

final case class DeleteResult(result: Long)

object DeleteResultProtocol extends DefaultJsonProtocol {
    implicit val deleteResultFormat = jsonFormat1(DeleteResult)
}
