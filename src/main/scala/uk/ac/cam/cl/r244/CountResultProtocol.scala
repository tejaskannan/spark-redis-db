package uk.ac.cam.cl.r244

import spray.json.DefaultJsonProtocol._
import spray.json._ 

final case class CountResult(result: Long, time: Double)

object CountResultProtocol extends DefaultJsonProtocol {
	implicit val countFormat = jsonFormat2(CountResult) 
}
