package uk.ac.cam.cl.r244

import spray.json.DefaultJsonProtocol._
import spray.json._ 

final case class ReadQuery(table: String, field: String, queryType: String,
						   target: String, multiWord: Option[Boolean], threshold: Option[Int])

object ReadQueryProtocol extends DefaultJsonProtocol {
	implicit val queryFormat = jsonFormat6(ReadQuery) 
}
