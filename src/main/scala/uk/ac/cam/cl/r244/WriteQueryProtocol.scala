package uk.ac.cam.cl.r244

import spray.json.DefaultJsonProtocol._
import spray.json._ 

final case class WriteQuery(table: String, id: String, record: Map[String, String])

object WriteQueryProtocol extends DefaultJsonProtocol {
	implicit val writeFormat = jsonFormat3(WriteQuery) 
}
