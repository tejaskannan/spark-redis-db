package uk.ac.cam.cl.r244

import spray.json.DefaultJsonProtocol._
import spray.json._ 

final case class CountResult(result: Long)

object CountResultProtocol extends DefaultJsonProtocol {
	implicit val countFormat = jsonFormat1(CountResult) 
}
