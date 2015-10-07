package reinvent.securityanalytics.profilers

import org.apache.commons.logging.LogFactory
import reinvent.securityanalytics.GeoIPLookup
import reinvent.securityanalytics.state.ActivityProfile
import reinvent.securityanalytics.utilities.{Configuration, GeoIPException}

/**A GeoIPCountryProfiler will alert you when AWS activity involving your AWS resources comes from a new country, as
  *    determined by a GeoIP database.*/
class GeoIPCountryProfiler(config:Configuration) extends ActivityProfiler[String] ("SourceIP-Country", config) {
  private val logger = LogFactory.getLog(this.getClass)
  val geoIPLookup = new GeoIPLookup(config)

  override def compareNewActivity(newActivity:Set[String], profile:ActivityProfile[String]):ActivityProfile[String]  = {
    try {
      val transformedNewActivity = newActivity.map((sourceIpAddress: String) => {
        geoIPLookup.countryLookup(sourceIpAddress)
      })
      compareNewTransformedActivity(transformedNewActivity, profile)
    }
    catch {
      case (g:GeoIPException) => {
        logger.error("Could not look up country for " + newActivity)
        new ActivityProfile(Set.empty)
      }
    }
  }
}
