package uk.ac.cam.cl.r244

import spray.json.DefaultJsonProtocol._
import spray.json._ 

final case class WriteResult(result: Boolean)

object WriteResultProtocol extends DefaultJsonProtocol {
	implicit val writeResultFormat = jsonFormat1(WriteResult) 
}
