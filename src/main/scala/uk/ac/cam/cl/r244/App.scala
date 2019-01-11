package uk.ac.cam.cl.r244

/**
 * @author tejas.kannan
 */

import scala.collection.immutable.{Map, List}
import scala.concurrent.Future
import scala.util.{Failure, Success}
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
import GetResultProtocol._
import DeleteQueryProtocol._
import DeleteResultProtocol._
import WriteQueryProtocol._
import WriteResultProtocol._
import BulkWriteQueryProtocol._
import BulkWriteResultProtocol._

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

        implicit val system = ActorSystem()
        implicit val materializer = ActorMaterializer()
        // needed for the future flatMap/onComplete in the end
        implicit val executionContext = system.dispatcher

        val route: Route =
            get {
                path("count") {
                    entity(as[String]) { queryStr =>
                        val jsonAst = queryStr.parseJson
                        val query = jsonAst.convertTo[ReadQuery]
                        val startTime = System.currentTimeMillis()
                        val count: Future[Long] = Future {
                            handleCount(query, db)
                        }
                        onSuccess(count) { value =>
                            val endTime = System.currentTimeMillis()
                            complete(CountResult(value, endTime - startTime).toJson.compactPrint)
                        }
                    }
                } ~
                path("get") {
                    entity(as[String]) { queryStr =>
                        val jsonAst = queryStr.parseJson
                        val query = jsonAst.convertTo[ReadQuery]
                        val startTime = System.currentTimeMillis()
                        val results: Future[List[Map[String, String]]] = Future {
                            handleGet(query, db)
                        }
                        onSuccess(results) { value =>
                            val endTime = System.currentTimeMillis()
                            complete(GetResult(value, endTime - startTime).toJson.compactPrint)
                        }
                    }
                }
            } ~
            post {
                path("delete") {
                    entity(as[String]) { queryStr => 
                        val jsonAst = queryStr.parseJson
                        val query = jsonAst.convertTo[DeleteQuery]
                        val result: Future[Long] = Future {
                            db.delete(query.table, query.id)
                        }
                        onSuccess(result) { value =>
                            complete(DeleteResult(value).toJson.compactPrint)
                        }
                    }
                } ~
                path("write") {
                    entity(as[String]) { queryStr =>
                        val jsonAst = queryStr.parseJson
                        val query = jsonAst.convertTo[WriteQuery]
                        val result: Future[Boolean] = Future {
                            db.write(query.table, query.id, query.record)
                        }
                        onSuccess(result) { value =>
                            complete(WriteResult(value).toJson.compactPrint)
                        }
                    }
                } ~
                path("bulkwrite") {
                    entity(as[String]) { queryStr =>
                        val jsonAst = queryStr.parseJson
                        val query = jsonAst.convertTo[BulkWriteQuery]
                        val result: Future[Long] = Future {
                            db.bulkWrite(query.table, query.records)
                        }
                        onSuccess(result) { value =>
                            complete(BulkWriteResult(value).toJson.compactPrint)
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

    def handleGet(query: ReadQuery, db: RedisDatabase): List[Map[String, String]] = {
        val queryType = query.queryType
        val multiWord = if (query.multiWord != None) query.multiWord.get else false
        val threshold = if (query.threshold != None) query.threshold.get else 0
        val resultFields = if (query.resultFields != None) query.resultFields.get else List[String]()
        queryType match {
            case `prefix` => db.getWithPrefix(query.table, query.field,
                                                    query.target, multiWord, resultFields)
            case `suffix` => db.getWithSuffix(query.table, query.field,
                                                    query.target, multiWord, resultFields)
            case `contains` => db.getWithContains(query.table, query.field,
                                                    query.target, multiWord, resultFields)
            case `regex` => db.getWithRegex(query.table, query.field,
                                                    query.target, multiWord, resultFields)
            case `editdist` => db.getWithEditDistance(query.table, query.field, query.target,
                                                    threshold, multiWord, resultFields)
            case `sw` => db.getWithSmithWaterman(query.table, query.field, query.target,
                                                    threshold, multiWord, resultFields)
            case _ => List[Map[String, String]]()
        }
    }
}
