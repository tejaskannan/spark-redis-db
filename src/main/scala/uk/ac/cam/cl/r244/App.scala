package uk.ac.cam.cl.r244

/**
 * @author ${user.name}
 */

import scala.collection.immutable.{Map, List}
import java.util.Random
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.io.StdIn
import ReadQueryProtocol._
import CountResultProtocol._
import scala.concurrent.Future
import scala.util.{Failure, Success}

object App {

    private val prefix = "prefix"
    private val suffix = "suffix"
    private val regex = "regex"
    private val editdist = "edit-distance"
    private val sw = "smith-waterman"
    private val contains = "contains"

    private val result = "result"

    def main(args : Array[String]) {
        val db = new RedisDatabase("localhost", 6379)

        implicit val system = ActorSystem("my-system")
        implicit val materializer = ActorMaterializer()
        // needed for the future flatMap/onComplete in the end
        implicit val executionContext = system.dispatcher

        val route: Route =
            get {
                path("search") {
                    entity(as[String]) { queryStr =>
                        val jsonAst = queryStr.parseJson
                        val query = jsonAst.convertTo[ReadQuery]
                        val count: Future[Long] = Future {
                            handleCount(query, db)
                        }
                        onSuccess(count) { value =>
                            complete(CountResult(value).toJson.prettyPrint)
                        }
                    }
                }
            }

        val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

        println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
        StdIn.readLine() // let it run until user presses return
        bindingFuture
          .flatMap(_.unbind()) // trigger unbinding from the port
          .onComplete(_ => system.terminate()) // and shutdown when done 
    }

    def handleCount(query: ReadQuery, db: RedisDatabase): Long = {
        val queryType = query.queryType
        val multiWord = if (query.multiWord != None) query.multiWord.get else false
        val threshold = if (query.threshold != None) query.threshold.get else 0
        queryType match {
            case `prefix` => db.countWithPrefix(query.table, query.field,
                                                    query.target, multiWord)
            case `suffix` => db.countWithSuffix(query.table, query.field,
                                                    query.target, multiWord)
            case `contains` => db.countWithContains(query.table, query.field,
                                                    query.target, multiWord)
            case `regex` => db.countWithRegex(query.table, query.field,
                                                    query.target, multiWord)
            case `editdist` => db.countWithEditDistance(query.table, query.field,
                                                    query.target, threshold, multiWord)
            case `sw` => db.countWithSmithWaterman(query.table, query.field,
                                                    query.target, threshold, multiWord)
            case _ => 0
        }
    }
}
