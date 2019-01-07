package uk.ac.cam.cl.r244

import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.collection.immutable.{List, Map}

final case class GetResult(result: List[Map[String, String]])

object GetResultProtocol extends DefaultJsonProtocol {
	implicit val getFormat = jsonFormat1(GetResult) 
}
