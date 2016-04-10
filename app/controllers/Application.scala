package controllers

import javax.inject.Inject

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.EventSource
import play.api.libs.json.Json
import play.api.mvc._
import services.TwitterStreamingService

class Application @Inject()(twitterStreamingService: TwitterStreamingService) extends Controller {

  implicit val system = ActorSystem("reactive-tweets")
  implicit val materializer = ActorMaterializer()

  def index = Action {
    val source = twitterStreamingService.messages
        .map(Json.parse)
          .map(json =>
            for {
              maybeName <- (json \ "user" \ "name").asOpt[String]
              maybeText <- (json \ "text").asOpt[String]
              maybeHashtags <- (json \ "entities" \ "hashtags").asOpt[Seq[String]]
            } yield s"@$maybeName: $maybeText"
          )
          .filter(_.nonEmpty)
          .map(_.get)
    Ok.chunked(source via EventSource.flow)
  }

}