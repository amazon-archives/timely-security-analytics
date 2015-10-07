package reinvent.securityanalytics.profilers

import org.apache.commons.logging.LogFactory
import reinvent.securityanalytics.GeoIPLookup
import reinvent.securityanalytics.state.ActivityProfile
import reinvent.securityanalytics.utilities.{Configuration, GeoIPException}

/**A GeoIPCityProfiler will alert you when AWS activity involving your AWS resources comes from a new city, as
  *    determined by a GeoIP database.*/
class GeoIPCityProfiler(config:Configuration) extends ActivityProfiler[String] ("SourceIP-City", config) {
  val geoIPLookup = new GeoIPLookup(config)
  private val logger = LogFactory.getLog(this.getClass)

  override def compareNewActivity(newActivity:Set[String], profile:ActivityProfile[String]):ActivityProfile[String] = {
    try {
      val transformedNewActivity = newActivity.map((sourceIpAddress: String) => {
        geoIPLookup.cityLookup(sourceIpAddress)
      })
      compareNewTransformedActivity(transformedNewActivity, profile)
    }
    catch {
      case (g:GeoIPException) => {
        logger.info("Could not look up city for " + newActivity)
        new ActivityProfile(Set.empty)
      }
    }
  }
}
