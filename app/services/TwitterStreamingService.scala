package services

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import javax.inject.Inject

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.event.Event
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants, Hosts, HttpHosts}
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}
import play.api.Configuration

import scala.collection.JavaConverters._

class TwitterStreamingService @Inject()(config: Configuration) {

  /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
  private val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue[String](100000)
  private val eventQueue: BlockingQueue[Event] = new LinkedBlockingQueue[Event](1000)

  private def getConnectionInfo(consumerKey: String, consumerSecret: String, token: String, secret: String)
  : (Hosts, StatusesFilterEndpoint, Authentication) = {
    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    val hosts: Hosts = new HttpHosts(Constants.STREAM_HOST)
    val endpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint()

    val terms = List("twitter", "api")
    endpoint.trackTerms(terms.asJava)

    // These secrets should be read from a config file
    val auth: Authentication = new OAuth1(consumerKey, consumerSecret, token, secret)
    (hosts, endpoint, auth)
  }

  private def createClient(hosts: Hosts, endpoints: StatusesFilterEndpoint, auth: Authentication): Client = {
    val builder: ClientBuilder = new ClientBuilder()
      .name("Hosebird-Client-01")                       // optional: mainly for the logs
      .hosts(hosts)
      .authentication(auth)
      .endpoint(endpoints)
      .processor(new StringDelimitedProcessor(msgQueue))
      .eventMessageQueue(eventQueue)                   // optional: use this if you want to process client events

    val client: Client = builder.build()
    // Attempts to establish a connection.
    client.connect()

    client
  }

  val (client: Option[Client], messages: Source[String, NotUsed], events: Source[Event, NotUsed]) = (for {
    twitterConfig <- config.getConfig("twitter")
    consumerKey <- twitterConfig.getString("consumerKey")
    consumerSecret <- twitterConfig.getString("consumerSecret")
    token <- twitterConfig.getString("token")
    secret <- twitterConfig.getString("secret")
  } yield {
    val (hosts, endpoint, auth) = getConnectionInfo(consumerKey, consumerSecret, token, secret)
    val client: Option[Client] = Some(createClient(hosts, endpoint, auth))
    val messages: Source[String, NotUsed] = Source.fromIterator(() => msgQueue.iterator().asScala)
    val events: Source[Event, NotUsed] = Source.fromIterator(() => eventQueue.iterator().asScala)
    (client, messages, events)
  }).getOrElse((None, Source.empty[String], Source.empty[Event]))




}
