package reinvent.securityanalytics

import java.net.{InetAddress, UnknownHostException}

import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import reinvent.securityanalytics.utilities.{Configuration, TorExitNodeList}

import scala.io.Source

//TODO: Update the exit node list periodically.
class TorExitLookup(config:Configuration) extends Serializable {
  private val logger = LogFactory.getLog(this.getClass)
  val EXIT_FILE_ADDRESS_LINE = "ExitAddress"
  var exitNodesListOption:Option[Broadcast[TorExitNodeList]] = None
  var lastUpdateTimestamp:Long = 0

  def getExitNodeRDD(sparkContext:SparkContext):RDD[InetAddress] = {
    loadExitNodeList() match {
      case None => sparkContext.parallelize(Seq.empty[InetAddress])
      case Some(exitNodesList) => {
        sparkContext.parallelize(exitNodesList.exitNodes.toSeq)
      }
    }
  }

  def initialize(sparkContext:SparkContext):Unit = {
    exitNodesListOption = Some(getBroadcastExitNodeList(sparkContext))
  }

  def getBroadcastExitNodeList(sparkContext:SparkContext):Broadcast[TorExitNodeList] = {
    val nodeList = loadExitNodeList() match {
      case None => new TorExitNodeList(Set.empty[InetAddress], 0)
      case Some(exitNodesList) => {
        exitNodesList
      }
    }
    sparkContext.broadcast(nodeList)
  }

  /*Format looks like:
    ExitNode 0011BD2485AD45D984EC4159C88FC066E5E3300E
    Published 2015-09-27 22:16:46
    LastStatus 2015-09-27 23:08:39
    ExitAddress 162.247.72.201 2015-09-27 23:17:58
    ExitNode 0098C475875ABC4AA864738B1D1079F711C38287
    Published 2015-09-28 13:59:30
    LastStatus 2015-09-28 15:03:16
    ExitAddress 162.248.160.151 2015-09-28 15:12:01
    ExitNode 00B70D1F261EBF4576D06CE0DA69E1F700598239
    Published 2015-09-28 10:21:07
    LastStatus 2015-09-28 11:02:17
    ExitAddress 193.34.116.18 2015-09-28 11:10:34
    ...

    We just want the IPs.  The following function gets them.
  */
  private def loadExitNodeList():Option[TorExitNodeList] = {
    try {
      val exitNodesDump = Source.fromURL(config.getString(Configuration.EXIT_NODE_URL)).getLines()
      val exitNodes = exitNodesDump.filter(line => line.contains(EXIT_FILE_ADDRESS_LINE)).map(line => {
        val lineArr = line.split(" ")
        if (lineArr == null || lineArr.length < 2) {
          logger.warn("The following line could not be converted to an IP string: " + line)
          None
        }
        else {
          val ipAddressString = lineArr(1)

          try {
            Some(InetAddress.getByName(ipAddressString))
          }
          catch {
            case (u: UnknownHostException) => {
              logger.warn("Could not parse " + ipAddressString + " into an IP address")
              None
            }
          }
        }
      }).filter(_.isDefined).map(_.get).toSet //Filter out the entries we couldn't parse

      if (exitNodes.size < 1) {
        logger.error("No exit node information found at " + config.getString(Configuration.EXIT_NODE_URL))
      }
      Some(new TorExitNodeList(exitNodes, System.currentTimeMillis()))
    }
    catch {
      case (e:Exception) => {
        logger.error("Problem initializing the Tor exit node list.  Continuing without initialization.", e)
        None
      }
    }
  }

  def isExitNode(ip:InetAddress):Boolean = {
    if (exitNodesListOption.isDefined) {
      val exitNodeList = exitNodesListOption.get.value.exitNodes
      if (exitNodeList.isEmpty) {
        logger.warn("Exit node list is empty.  Make sure to initialize it first.")
        false
      }
      else {
        exitNodeList.contains(ip)
      }
    }
    else {
      logger.error("Error: Exit node list not initialized.")
      false
    }
  }
}
