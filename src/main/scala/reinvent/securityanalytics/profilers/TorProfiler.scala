package reinvent.securityanalytics.profilers

import java.net.InetAddress
import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkContext
import reinvent.securityanalytics.TorExitLookup
import reinvent.securityanalytics.state.ActivityProfile
import reinvent.securityanalytics.utilities.{Configuration, SNSSingleton}

/**A TorProfiler which will alert you when AWS activity involving your AWS resources originates from the Tor anonymizing
  *    network.  While this may be fine for some administrators, this may be unacceptable to others who would see the
  *    use of an anonymizing proxy as a sign of hiding one's tracks.*/
class TorProfiler(config:Configuration) extends ActivityProfiler[String] ("SourceIP-TorExitNode", config) {
  private val logger = LogFactory.getLog(this.getClass)

  val torExitLookup = new TorExitLookup(config)

  override def initialize(sparkContext:SparkContext) = {
    logger.info("Initializing TorProfiler")
    torExitLookup.initialize(sparkContext) //Load the exit node list.
  }

  override def compareNewActivity(newActivity:Set[String], profile:ActivityProfile[String]):ActivityProfile[String] = {
    var updatedProfile = profile
    newActivity.foreach((sourceIpAddress:String) => {
      try {
        val ipAddress = InetAddress.getByName(sourceIpAddress)
        if (torExitLookup.isExitNode(ipAddress)) {
          val sns = SNSSingleton.client(config.getString(Configuration.REGION))
          val subject = "Anonymizing proxy in use with your AWS account"
          val message = "Activity on your AWS resources has been seen from " + sourceIpAddress + " which is a Tor exit node."
          sns.publish(config.getString(Configuration.ALERT_TOPIC), message, subject)
          /*TODO Currently this will generate an alarm for each remote IP
            Instead, we should track either the long-term credential or the principalID to avoid duplicates.  In which case,
            an expiration scheme is needed. */
          updatedProfile = profile.addNewActivity(Set.empty) //Will increment alerts counter.
        }
      }
      catch {
        case (u:Throwable) => {
          logger.error("Invalid IP address: " + sourceIpAddress, u)
        }
        case (e:Exception) => {
          logger.error("Unexpected exception while parsing IP address", e)
        }
      }
    })
    updatedProfile
  }
}
